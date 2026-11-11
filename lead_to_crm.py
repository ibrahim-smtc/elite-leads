"""
lead_to_crm.py  -  MongoDB og-leads  ->  Perfox agent (AI picks the relevant fields)  ->  CRM customer profile

Flow:
  1. find leads that are new or changed since they were last synced (hash of the lead data)
  2. leads are queued in groups of CRM_AGENT_BATCH (default 5); for each group:
       a. code: create/find each customer in the CRM (name, phone, email, external_id)
       b. ONE agent call for the whole group (Perfox workflow 'customer-updates', over its webhook):
          the agent receives only the fields that have a value, decides which few are relevant to a
          customer profile (usually 5-6 of ~70), and writes them with update_customer_field
       c. code: read the agent's JSON report, check every field was accounted for, and save it on
          each lead in MongoDB (crm_sync)

Triggered automatically by POST /sync (app.py) right after the sheet is uploaded to MongoDB, via
process_pending(). It can also be run by hand, as below.

Safe by default: without --live nothing is sent to the CRM / agent and nothing is written to MongoDB.

Safety cap: at most CRM_MAX_PER_RUN leads (default 10) are processed per run, oldest first.
Leads beyond the cap stay pending for the next run. CRM_MAX_PER_RUN=0 means no cap.

Usage
  python lead_to_crm.py --show --limit 3        # dry run: shows what would be sent for 3 pending leads
  python lead_to_crm.py --live --limit 1        # real run on a single lead
  python lead_to_crm.py --live                  # real run: up to CRM_MAX_PER_RUN pending leads (default 10)
  python lead_to_crm.py --live --all            # ignore the cap: every pending lead (costs many tokens)
  python lead_to_crm.py --live --batch 5        # queue size: 5 leads per agent call (default)
  python lead_to_crm.py --live --watch 60       # keep polling every 60s for new/changed rows
  python lead_to_crm.py --phone 9380810782 --live --force
  python lead_to_crm.py --baseline --live       # mark leads already in Mongo as handled: only NEW/EDITED rows reach the agent
  python lead_to_crm.py --unbaseline --live     # undo that

Secrets (never printed, never written to a file):
  MONGODB_URI                      sheet-sync/.env
  PERFOX_API_KEY                   environment, else read at runtime from the Perfox plugin's .mcp.json
  PERFOX_AGENT_WEBHOOK_URL         the trigger's Full URL (default below)
  PERFOX_AGENT_WEBHOOK_SECRET      the trigger's shared secret (generate it in Studio)
"""
import argparse
import glob
import hashlib
import json
import os
import re
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import requests
from dotenv import load_dotenv
from pymongo import MongoClient

HERE = Path(__file__).resolve().parent
load_dotenv(HERE / ".env")

# NOTE: deliberately NOT using MONGODB_DB / MONGODB_COLLECTION - this .env points those at
# crm_staging.elite_vw_leads (used by sync.py), which does not exist on the cluster.
LEADS_DB = os.getenv("LEADS_DB", "elite-test-db")
LEADS_COLLECTION = os.getenv("LEADS_COLLECTION", "og-leads")
PERFOX_MCP_URL = os.getenv("PERFOX_MCP_URL", "https://elite-motors-api.perfox.ai/mcp")
AGENT_WEBHOOK_URL = os.getenv(
    "PERFOX_AGENT_WEBHOOK_URL",
    "https://elite-motors-api.perfox.ai/hooks/01a1c523-fb77-73e0-87da-8da8786d701f/lead-profile",
)
MAX_PER_RUN = int(os.getenv("CRM_MAX_PER_RUN", "10"))  # most leads the agent processes per run (token safety); 0 = no cap
BATCH_SIZE = int(os.getenv("CRM_AGENT_BATCH", "5"))      # leads per agent call (the queue); fewer calls = fewer tokens
MAX_RETRIES_PER_LEAD = 3
# The CRM needs a little while before it can see a customer that was JUST created. Wait this long
# (counted from the customer's creation) before the agent first looks for it; leads the CRM still
# cannot find are retried after 1x, then 2x this long, instead of being marked failed.
SETTLE_SECONDS = int(os.getenv("CRM_SETTLE_SECONDS", "30"))
RUN_LOG = HERE / "crm_agent_runs.jsonl"

# Bookkeeping, not lead data: never sent to the agent.
NON_LEAD_FIELDS = {"_id", "crm_sync"}

# Call/admin columns: they describe the calling process, not the person. The agent always skipped
# them, so code handles them (no tokens spent) and they do not count as a profile change either.
ADMIN_FIELDS = ("date", "last_updated", "updated_by", "attempt", "most_recent_status", "campaign_name")

# A lead goes back to the agent only when its PROFILE data changes. Not counted as a change:
# bookkeeping, the row's position in the sheet (_source: inserting one row shifts every row number),
# duplicates, and the call/admin columns above (the call system rewrites those all the time).
HASH_IGNORE = NON_LEAD_FIELDS | {"synced_at", "first_synced_at", "_source", "phone_raw", "sheetid"} | set(ADMIN_FIELDS)
HASH_VERSION = 2  # bump whenever the rule above changes

# Fields code handles itself (so the agent never has to): field -> reason
CODE_HANDLED = {
    "name": "saved as the customer's name",
    "phone": "saved as the customer's phone",
    "email": "saved as the customer's email",
    "phone_raw": "duplicate of phone",
    "sheetid": "internal sheet reference, not lead information",
    "_source": "ingestion bookkeeping",
    "synced_at": "ingestion bookkeeping",
    "first_synced_at": "ingestion bookkeeping",
}
CODE_HANDLED.update({k: "call or admin data, not profile information" for k in ADMIN_FIELDS})


# --------------------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------------------
def utcnow():
    return datetime.now(timezone.utc)


def to_text(v):
    if isinstance(v, datetime):
        return v.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (dict, list)):
        return json.dumps(v, default=str, ensure_ascii=False)
    return str(v).strip()


def available_fields(doc):
    """Only fields that actually have a value (a lead has ~17 of ~70 filled)."""
    out = {}
    for k, v in doc.items():
        if k in NON_LEAD_FIELDS or v is None:
            continue
        if isinstance(v, str) and not v.strip():
            continue
        if isinstance(v, (dict, list)) and not v:
            continue
        out[k] = to_text(v)
    return out


def lead_hash(doc):
    core = {k: to_text(v) for k, v in doc.items() if k not in HASH_IGNORE and v is not None}
    return hashlib.sha1(json.dumps(core, sort_keys=True, default=str).encode()).hexdigest()


def digits(s):
    return re.sub(r"\D", "", str(s or ""))


def norm_phone(s):
    d = digits(s)
    if not d:
        return None
    return "+91" + d if len(d) == 10 else "+" + d


EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


def mask(s, keep=2):
    s = str(s or "")
    return s if len(s) <= keep * 2 else s[:keep] + "***" + s[-keep:]


# --------------------------------------------------------------------------------------
# Perfox platform MCP client (JSON-RPC over HTTP) - creates the customer + tags
# --------------------------------------------------------------------------------------
def perfox_auth_header():
    key = os.getenv("PERFOX_API_KEY")
    if key:
        return key if key.lower().startswith("bearer ") else f"Bearer {key}"
    pattern = str(Path.home() / ".claude" / "plugins" / "synced" / "*" / "perfox-elite-motors-mcp" / ".mcp.json")
    for p in glob.glob(pattern):
        try:
            cfg = json.loads(Path(p).read_text(encoding="utf-8"))
            for srv in cfg.get("mcpServers", {}).values():
                h = srv.get("headers", {}).get("Authorization")
                if h:
                    return h if h.lower().startswith("bearer ") else f"Bearer {h}"
        except Exception:
            continue
    raise RuntimeError("No Perfox key: set PERFOX_API_KEY or install the Perfox plugin (.mcp.json).")


class PerfoxError(RuntimeError):
    pass


class PerfoxMCP:
    def __init__(self, url=PERFOX_MCP_URL):
        self.url = url
        self.s = requests.Session()
        self.s.headers.update({
            "Authorization": perfox_auth_header(),
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
        })
        self._id = 0
        self._ready = False

    def _post(self, payload, expect_response=True):
        r = self.s.post(self.url, json=payload, timeout=60)
        if r.status_code in (401, 403):
            raise PerfoxError(f"Perfox auth failed (HTTP {r.status_code})")
        sid = r.headers.get("Mcp-Session-Id")
        if sid:
            self.s.headers["Mcp-Session-Id"] = sid
        if not expect_response:
            return None
        r.raise_for_status()
        if "text/event-stream" in r.headers.get("Content-Type", ""):
            for line in r.text.splitlines():
                if line.startswith("data:"):
                    try:
                        msg = json.loads(line[5:].strip())
                    except ValueError:
                        continue
                    if msg.get("id") == payload.get("id"):
                        return msg
            raise PerfoxError("No JSON-RPC response in event stream")
        return r.json()

    def _rpc(self, method, params=None):
        self._id += 1
        msg = self._post({"jsonrpc": "2.0", "id": self._id, "method": method, "params": params or {}})
        if "error" in msg:
            raise PerfoxError(json.dumps(msg["error"])[:500])
        return msg["result"]

    def _init(self):
        if self._ready:
            return
        self._rpc("initialize", {
            "protocolVersion": "2025-03-26", "capabilities": {},
            "clientInfo": {"name": "lead_to_crm", "version": "2.0"},
        })
        self._post({"jsonrpc": "2.0", "method": "notifications/initialized"}, expect_response=False)
        self._ready = True

    def call(self, tool, args=None):
        self._init()
        res = self._rpc("tools/call", {"name": tool, "arguments": args or {}})
        text = "".join(c.get("text", "") for c in res.get("content", []) if c.get("type") == "text")
        if res.get("isError"):
            raise PerfoxError(text[:500] or "tool error")
        if "structuredContent" in res:
            return res["structuredContent"]
        try:
            return json.loads(text)
        except ValueError:
            return text


def created_ts_of(res):
    """When the customer record was created (epoch seconds, from the upsert answer), or None."""
    if isinstance(res, dict):
        v = res.get("created_at")
        if isinstance(v, str):
            try:
                return datetime.fromisoformat(v.replace("Z", "+00:00")).timestamp()
            except ValueError:
                return None
        for x in res.values():
            if isinstance(x, dict):
                got = created_ts_of(x)
                if got:
                    return got
    return None


def crm_id_of(res):
    if isinstance(res, dict):
        for k in ("id", "customer_id"):
            if res.get(k):
                return res[k]
        for v in res.values():
            if isinstance(v, dict):
                got = crm_id_of(v)
                if got:
                    return got
    return None


# --------------------------------------------------------------------------------------
# Step 1: identity + tags (deterministic)
# --------------------------------------------------------------------------------------
def build_identity(fields):
    name = fields.get("name") or " ".join(x for x in (fields.get("first_name"), fields.get("last_name")) if x) or None
    phone = norm_phone(fields.get("phone") or fields.get("enriched_phone"))
    email = (fields.get("email") or "").strip().lower()
    if not EMAIL_RE.match(email):
        email = (fields.get("enriched_email") or "").strip().lower()
    email = email if EMAIL_RE.match(email) else None
    return {"name": name, "phone": phone, "email": email, "preferred_language": "en"}


def upsert_with_conflict_fallback(mcp, identity, lead_id):
    """
    Perfox refuses identifiers that match two different customers (identity_conflict).
    Try full identity, then drop the email, then drop the phone. Never auto-merges people.
    Returns (response, note, identity_actually_used).
    """
    base = {k: v for k, v in identity.items() if v not in (None, "", [], {})}
    attempts = [("full", dict(base, external_id=lead_id))]
    if "email" in base:
        attempts.append(("without_email", {**{k: v for k, v in base.items() if k != "email"}, "external_id": lead_id}))
    if "phone" in base:
        attempts.append(("without_phone", {**{k: v for k, v in base.items() if k != "phone"}, "external_id": lead_id}))
    last_err = None
    for label, args in attempts:
        try:
            res = mcp.call("upsert_customer", args)
            return res, (None if label == "full" else f"identity_conflict resolved via {label}"), args
        except PerfoxError as e:
            last_err = e
            if "identity_conflict" not in str(e) and "409" not in str(e):
                raise
    raise PerfoxError(f"all identity attempts conflicted: {last_err}")


# --------------------------------------------------------------------------------------
# Step 2: the Perfox agent decides which fields matter and writes them to the profile
# --------------------------------------------------------------------------------------
def parse_report(text):
    """The agent replies with one JSON object; tolerate fences or stray prose around it."""
    if not isinstance(text, str) or not text.strip():
        raise ValueError("empty agent reply")
    t = re.sub(r"```(?:json)?", "", text)
    i, j = t.find("{"), t.rfind("}")
    if i < 0 or j <= i:
        raise ValueError(f"no JSON in agent reply: {text[:200]!r}")
    return json.loads(t[i:j + 1])


TRANSIENT_MARKERS = ("changed by somebody", "conflict", "reload and try again", "concurrent")


def has_transient(report):
    """The CRM rejects a write if the customer changed a moment ago (optimistic locking)."""
    items = (report.get("skipped") or []) + (report.get("no_home") or [])
    return any(m in str(x.get("reason", "")).lower() for x in items if isinstance(x, dict) for m in TRANSIENT_MARKERS)


class AgentConfigError(RuntimeError):
    """Wrong secret / URL / trigger settings: retrying will not help."""


def run_agent_batch(ctxs, attempt=0):
    """One webhook call = one agent run for SEVERAL leads (the queue's batch)."""
    secret = os.getenv("PERFOX_AGENT_WEBHOOK_SECRET")
    if not secret:
        raise AgentConfigError("PERFOX_AGENT_WEBHOOK_SECRET missing - generate it on the webhook trigger in Studio.")
    payload = {"leads": [{
        "lead_id": c["lead_id"],
        "name": c["identity_used"].get("name"),
        "phone": c["identity_used"].get("phone"),
        "email": c["identity_used"].get("email"),
        "available_fields": c["agent_fields"],
    } for c in ctxs]}
    seed = "|".join(f"{c['lead_id']}:{c['record']['hash'][:8]}:{c['record']['tries']}" for c in ctxs)
    headers = {
        "Content-Type": "application/json",
        "X-Webhook-Secret": secret,
        "Idempotency-Key": f"batch:{hashlib.sha1(seed.encode()).hexdigest()[:16]}:{attempt}",  # a retry of THIS delivery runs once
    }
    r = requests.post(AGENT_WEBHOOK_URL, json=payload, headers=headers, timeout=900)
    if r.status_code in (401, 403):
        raise AgentConfigError(f"webhook rejected the secret (HTTP {r.status_code})")
    if r.status_code == 404:
        raise AgentConfigError("webhook not found (HTTP 404) - is the agent Activated and the path 'lead-profile'?")
    if r.status_code == 202:
        raise AgentConfigError("webhook answered 202 (no reply). Set the trigger's Response to 'Wait for reply'.")
    r.raise_for_status()
    body = r.json()
    if "response_text" not in body:
        raise AgentConfigError("no response_text from webhook (Response must be 'Wait for reply', auth must be on)")
    data = parse_report(body["response_text"])
    results = data.get("results")
    if results is None and len(ctxs) == 1 and "updated" in data:  # tolerate a bare single report
        results = [dict(data, lead_id=ctxs[0]["lead_id"])]
    by_id = {rep["lead_id"]: rep for rep in results or [] if isinstance(rep, dict) and rep.get("lead_id")}
    return by_id, body.get("execution_id")


def account_for(report, agent_fields):
    """Every field sent must be accounted for as updated / skipped / no_home."""
    def names(key):
        return [x.get("lead_field") for x in (report.get(key) or []) if isinstance(x, dict) and x.get("lead_field")]
    addressed = set(names("updated")) | set(names("skipped")) | set(names("no_home"))
    missing = [k for k in agent_fields if k not in addressed]
    skipped = list(report.get("skipped") or [])
    warnings = []
    if missing:
        warnings.append(f"{len(missing)} field(s) not accounted for by the agent: {missing[:8]}")
        skipped += [{"lead_field": k, "reason": "not accounted for by agent"} for k in missing]
    return skipped, warnings


# --------------------------------------------------------------------------------------
# Main loop
# --------------------------------------------------------------------------------------
def needs_sync(doc, force, retry_failed):
    cs = doc.get("crm_sync") or {}
    if force or not cs:
        return True
    if cs.get("status") in ("failed", "agent_failed", "skipped_no_contact"):
        may_retry = retry_failed and cs.get("tries", 0) < MAX_RETRIES_PER_LEAD
        return may_retry or cs.get("hash") != lead_hash(doc)
    return cs.get("hash") != lead_hash(doc)


def baseline(col, live):
    """
    Mark the leads that already exist as 'handled', so only NEW or EDITED rows reach the agent.
      - lead with no crm_sync yet           -> crm_sync = {status: "baseline", hash}   (undo: unbaseline)
      - lead synced under the old hash rule -> only its stored hash is refreshed
      - failed / agent_failed / skipped_no_contact leads are left alone (they are meant to retry)
    Dry run unless live.
    """
    counts = {"baselined": 0, "rehashed": 0, "left_alone": 0}
    for d in col.find({}):
        cs, h = d.get("crm_sync"), lead_hash(d)
        if not cs:
            counts["baselined"] += 1
            if live:
                col.update_one({"_id": d["_id"], "crm_sync": {"$exists": False}}, {"$set": {"crm_sync": {
                    "status": "baseline", "hash": h, "hash_version": HASH_VERSION, "tries": 0,
                    "baselined_at": utcnow(),
                    "note": "existing lead marked as handled; processed only if its profile data changes"}}})
        elif cs.get("status") == "synced" and cs.get("hash_version") != HASH_VERSION:
            counts["rehashed"] += 1
            if live:
                col.update_one({"_id": d["_id"]}, {"$set": {"crm_sync.hash": h, "crm_sync.hash_version": HASH_VERSION}})
        else:
            counts["left_alone"] += 1
    return counts


def unbaseline(col, live):
    """Undo baseline: leads still marked 'baseline' become pending again. Dry run unless live."""
    n = col.count_documents({"crm_sync.status": "baseline"})
    if live and n:
        col.update_many({"crm_sync.status": "baseline"}, {"$unset": {"crm_sync": ""}})
    return {"unbaselined": n}


def chunks(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


def failed_ctx(doc, err):
    prev = (doc.get("crm_sync") or {}).get("tries", 0)
    return {"doc": doc, "lead_id": doc["_id"], "done": True,
            "view": {"identity": {"name": None, "phone": None, "email": None}, "agent_fields": {}, "handled": {}},
            "record": {"status": "failed", "error": str(err)[:500], "hash": lead_hash(doc),
                       "hash_version": HASH_VERSION, "tries": prev + 1,
                       "available_count": 0, "sent_to_agent": 0, "attempted_at": utcnow()}}


def prepare_lead(doc, args, mcp):
    """Code-only part of a lead: filter fields, create/find the customer. No tokens used."""
    lead_id = doc["_id"]
    fields = available_fields(doc)
    prev = doc.get("crm_sync") or {}
    identity = build_identity(fields)
    handled = {k: why for k, why in CODE_HANDLED.items() if k in fields}
    agent_fields = {k: v for k, v in fields.items() if k not in CODE_HANDLED}
    record = {"hash": lead_hash(doc), "hash_version": HASH_VERSION, "tries": prev.get("tries", 0) + 1,
              "available_count": len(fields),
              "sent_to_agent": len(agent_fields), "handled_by_code": handled, "attempted_at": utcnow()}
    ctx = {"doc": doc, "lead_id": lead_id, "identity": identity, "identity_used": identity,
           "agent_fields": agent_fields, "record": record, "done": False, "report": None,
           "updated": [], "execution_id": None, "created_ts": None,
           "view": {"identity": identity, "agent_fields": agent_fields, "handled": handled}}

    if not (identity["phone"] or identity["email"]):
        record.update(status="skipped_no_contact", error="no phone or email available")
        ctx["done"] = True
    elif not args.live:
        record.update(status="dry_run")
        ctx["done"] = True
    else:
        try:
            res, note, used = upsert_with_conflict_fallback(mcp, identity, lead_id)
        except Exception as e:
            record.update(status="failed", error=str(e)[:500])
            ctx["done"] = True
            return ctx
        record.update(crm_id=crm_id_of(res), conflict_note=note)
        ctx["identity_used"] = used
        ctx["created_ts"] = created_ts_of(res)
        if not agent_fields:  # nothing left for the agent to judge: no tokens spent
            record.update(status="synced", synced_at=utcnow(), error=None, updated=[], skipped=[], no_home=[],
                          warnings=[], note="no fields needed the agent")
            ctx["done"] = True
    return ctx


def find_result(by_id, lead_id):
    rep = by_id.get(lead_id)
    if rep is None:  # tolerate a shortened id echoed back by the agent
        rep = next((v for k, v in by_id.items() if len(k) >= 8 and lead_id.startswith(k)), None)
    return rep


def fail(ctxs, err):
    for c in ctxs:
        c["record"].update(status="agent_failed", error=str(err)[:500])
        c["done"] = True


def run_agent_for(ctxs):
    """
    Send the queued leads to the agent in ONE call. Leads that come back with a transient CRM
    conflict ("changed a moment ago") or no result at all are re-queued, up to 3 rounds.
    """
    pending = list(ctxs)
    for attempt in range(3):
        if not pending:
            break
        last = attempt == 2
        try:
            by_id, execution_id = run_agent_batch(pending, attempt)
        except AgentConfigError as e:
            fail(pending, e)
            return
        except Exception as e:  # timeout / 5xx / unparseable reply: re-send the batch
            if last:
                fail(pending, e)
                return
            time.sleep(3 * (attempt + 1))
            continue
        retry = []
        for c in pending:
            rep = find_result(by_id, c["lead_id"])
            if rep is None:
                if last:
                    fail([c], "agent returned no result for this lead")
                else:
                    retry.append(c)
                continue
            c["report"], c["execution_id"] = rep, execution_id
            c["updated"] += [u for u in rep.get("updated") or [] if u not in c["updated"]]
            # "changed a moment ago" or "customer not found" (the CRM may not see a brand-new customer yet)
            if (has_transient(rep) or rep.get("status") == "customer_not_found") and not last:
                retry.append(c)
        pending = retry
        if pending:
            not_found = [c for c in pending if (c.get("report") or {}).get("status") == "customer_not_found"]
            wait = SETTLE_SECONDS * (attempt + 1) if not_found else 2
            if not_found:
                print(f"   -> {len(not_found)} lead(s) not visible in the CRM yet; waiting {wait}s, then asking again")
            time.sleep(wait)


def finish_lead(c):
    rep, rec = c["report"], c["record"]
    rep["updated"] = c["updated"]  # keep what earlier rounds wrote
    written = {u.get("lead_field") for u in c["updated"]}
    rep["skipped"] = [s for s in rep.get("skipped") or [] if s.get("lead_field") not in written]
    skipped, warnings = account_for(rep, c["agent_fields"])
    status = "failed" if rep.get("status") == "customer_not_found" else "synced"
    rec.update(
        status=status, synced_at=utcnow(), error=None if status == "synced" else "agent could not find the customer",
        execution_id=c["execution_id"], crm_id=rep.get("customer_id") or rec.get("crm_id"),
        updated=[{"crm_field": u.get("crm_field"), "lead_field": u.get("lead_field")} for u in rep.get("updated") or []],
        skipped=skipped, no_home=rep.get("no_home") or [], custom_fields_seen=rep.get("custom_fields_seen") or [],
        warnings=warnings,
    )
    c["view"]["report"] = rep
    c["done"] = True


def show(view, record, unmask):
    ident = dict(view["identity"])
    if not unmask:
        ident["name"], ident["phone"], ident["email"] = mask(ident["name"]), mask(ident["phone"], 3), mask(ident["email"])
    print(f"   identity (code): {json.dumps(ident, ensure_ascii=False)}")
    print(f"   handled by code ({len(view['handled'])}): {', '.join(view['handled'])}")
    print(f"   sent to agent  ({len(view['agent_fields'])}): ")
    for k, v in view["agent_fields"].items():
        print(f"      {k}: {v[:70]}")
    rep = view.get("report")
    if rep:
        print(f"   agent report: updated={len(rep.get('updated') or [])} skipped={len(rep.get('skipped') or [])} "
              f"no_home={len(rep.get('no_home') or [])}")
        for u in rep.get("updated") or []:
            print(f"      + {u.get('crm_field')} <- {u.get('lead_field')}: {str(u.get('value', ''))[:300]}")
        for s in rep.get("skipped") or []:
            print(f"      - skipped {s.get('lead_field')}: {s.get('reason')}")
        for s in rep.get("no_home") or []:
            print(f"      ? no home {s.get('lead_field')}")
    for w in record.get("warnings") or []:
        print("   ! warning:", w)
    if record.get("error"):
        print("   ! error:", record["error"])


def run_batch(args, col, mcp):
    query = {}
    ids = getattr(args, "ids", None)
    if ids:
        query = {"_id": {"$regex": "^(" + "|".join(re.escape(i.strip()) for i in ids.split(",") if i.strip()) + ")"}}
    if args.phone:
        query = {"phone": {"$regex": re.escape(digits(args.phone)[-10:]) + "$"}}
    docs = [d for d in col.find(query).sort("date", 1) if needs_sync(d, args.force, args.retry_failed)]
    limit = 0 if args.all else args.limit
    if limit:
        docs = docs[:limit]
    batch_size = max(1, getattr(args, "batch", None) or BATCH_SIZE)
    print(f"== {len(docs)} lead(s) queued, {batch_size} per agent call")

    counts, n = {}, 0
    for chunk in chunks(docs, batch_size):
        ctxs = []
        for doc in chunk:  # code-only prep: customer + filtered fields (no tokens)
            try:
                ctxs.append(prepare_lead(doc, args, mcp))
            except Exception as e:
                ctxs.append(failed_ctx(doc, e))
        todo = [c for c in ctxs if not c["done"]]
        if todo:  # ONE agent call for the whole chunk
            fresh = [c["created_ts"] for c in todo if c.get("created_ts")]
            if fresh and SETTLE_SECONDS > 0:  # give the CRM time to see customers created just now
                wait = SETTLE_SECONDS - (time.time() - max(fresh))
                if wait > 0:
                    print(f"   -> waiting {wait:.0f}s so the CRM can see the new customers")
                    time.sleep(wait)
            print(f"   -> {len(todo)} lead(s) sent to the agent in one call")
            try:
                run_agent_for(todo)
                for c in todo:
                    if not c["done"]:
                        finish_lead(c)
            except Exception as e:
                fail([c for c in todo if not c["done"]], e)

        for c in ctxs:
            n += 1
            rec, status = c["record"], c["record"]["status"]
            extra = (f" updated={len(rec.get('updated') or [])} skipped={len(rec.get('skipped') or [])} "
                     f"no_home={len(rec.get('no_home') or [])}") if "updated" in rec else \
                    f" sent_to_agent={rec.get('sent_to_agent', 0)}"
            print(f"[{n}/{len(docs)}] {c['lead_id'][:8]} {status}{extra}")
            if args.show:
                show(c["view"], rec, args.unmask)
            if args.live:
                col.update_one({"_id": c["lead_id"]}, {"$set": {"crm_sync": rec}})
            with RUN_LOG.open("a", encoding="utf-8") as f:
                f.write(json.dumps({"ts": utcnow().isoformat(), "lead": c["lead_id"], "status": status, "live": args.live,
                                    "updated": rec.get("updated"), "skipped": rec.get("skipped"),
                                    "no_home": rec.get("no_home"), "warnings": rec.get("warnings"),
                                    "error": rec.get("error")}, ensure_ascii=False, default=str) + "\n")
            counts[status] = counts.get(status, 0) + 1
        time.sleep(args.sleep)
    print("== done:", counts)
    return counts


_RUN_LOCK = threading.Lock()    # held while a CRM run is in progress
_STATE_LOCK = threading.Lock()  # guards _QUEUED and the hand-over between runs
_QUEUED = set()                 # (db, collection) that received a sync while a run was busy


def _mongo_collection(db_name, collection_name):
    uri = os.getenv("MONGODB_URI")
    if not uri:
        raise RuntimeError("MONGODB_URI missing")
    client = MongoClient(uri, serverSelectionTimeoutMS=15000)
    return client, client[db_name or LEADS_DB][collection_name or LEADS_COLLECTION]


def count_pending(db_name=None, collection_name=None):
    """How many leads a run would process right now (new, changed, or retryable). Read-only."""
    client, col = _mongo_collection(db_name, collection_name)
    try:
        return sum(1 for d in col.find({}) if needs_sync(d, False, True))
    finally:
        client.close()


def _run_once(db_name, collection_name, live, force):
    client = None
    try:
        client, col = _mongo_collection(db_name, collection_name)
        args = SimpleNamespace(live=live, limit=MAX_PER_RUN, all=False, phone=None, force=force, batch=BATCH_SIZE,
                               retry_failed=True, show=False, unmask=False, sleep=0.5, watch=0)
        return {"status": "done", "counts": run_batch(args, col, PerfoxMCP() if live else None)}
    except Exception as e:
        print(f"process_pending failed: {e}")
        return {"status": "failed", "error": str(e)[:300]}
    finally:
        if client:
            client.close()


def process_pending(db_name=None, collection_name=None, live=True, force=False):
    """
    Importable entry point (called by POST /sync after the upload): processes every pending lead
    (new or changed), in queued groups. Only one run at a time. If a sync arrives while a run is
    busy, it is remembered and the running worker goes round again when it finishes, so rows from
    that sync are never dropped.
    """
    with _STATE_LOCK:
        if not _RUN_LOCK.acquire(blocking=False):
            _QUEUED.add((db_name, collection_name))
            return {"status": "queued", "reason": "a CRM run is in progress; it will pick these rows up when it finishes"}
    released = False
    try:
        targets, first = [(db_name, collection_name)], True
        while True:
            for db, name in targets:
                result = _run_once(db, name, live, force and first)
            first = False
            with _STATE_LOCK:
                if not _QUEUED:
                    _RUN_LOCK.release()
                    released = True
                    return result
                targets = list(_QUEUED)
                _QUEUED.clear()
    finally:
        if not released:
            _RUN_LOCK.release()


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--live", action="store_true", help="really create customers, call the agent, write MongoDB")
    ap.add_argument("--limit", type=int, default=MAX_PER_RUN,
                    help="max leads this run (default: CRM_MAX_PER_RUN = 10; 0 = no cap)")
    ap.add_argument("--all", action="store_true", help="ignore any cap and process every pending lead")
    ap.add_argument("--batch", type=int, default=BATCH_SIZE,
                    help=f"leads per agent call (default {BATCH_SIZE}); 1 = one lead at a time")
    ap.add_argument("--db", help=f"database (default {LEADS_DB})")
    ap.add_argument("--collection", help=f"collection (default {LEADS_COLLECTION})")
    ap.add_argument("--ids", help="only leads whose _id starts with one of these (comma-separated prefixes)")
    ap.add_argument("--phone", help="only the lead whose phone ends with these digits")
    ap.add_argument("--force", action="store_true", help="ignore change detection")
    ap.add_argument("--retry-failed", action="store_true")
    ap.add_argument("--baseline", action="store_true",
                    help="mark leads already in Mongo as handled, so only NEW or EDITED rows reach the agent")
    ap.add_argument("--unbaseline", action="store_true", help="undo --baseline")
    ap.add_argument("--watch", type=int, default=0, metavar="SECONDS", help="keep polling for new/changed rows")
    ap.add_argument("--show", action="store_true", help="print what is sent / what the agent reported (identity masked)")
    ap.add_argument("--unmask", action="store_true", help="with --show, print real name/phone/email")
    ap.add_argument("--sleep", type=float, default=0.5, help="seconds between leads")
    args = ap.parse_args()

    uri = os.getenv("MONGODB_URI")
    if not uri:
        sys.exit("MONGODB_URI missing (check sheet-sync/.env).")
    client = MongoClient(uri, serverSelectionTimeoutMS=15000)
    db_name, col_name = args.db or LEADS_DB, args.collection or LEADS_COLLECTION
    col = client[db_name][col_name]
    if args.baseline or args.unbaseline:
        try:
            label = "baseline" if args.baseline else "unbaseline"
            print(f"== {label} on {db_name}.{col_name}:",
                  (baseline if args.baseline else unbaseline)(col, args.live))
            if not args.live:
                print("(dry run: nothing written; add --live to apply)")
        finally:
            client.close()
        return
    mcp = PerfoxMCP() if args.live else None
    print(f"== {'LIVE' if args.live else 'DRY-RUN (no writes, no agent calls)'} | source={db_name}.{col_name}")

    try:
        while True:
            run_batch(args, col, mcp)
            if not args.watch:
                break
            print(f"== waiting {args.watch}s for new or changed rows (Ctrl+C to stop)")
            time.sleep(args.watch)
    except KeyboardInterrupt:
        print("\nstopped")
    finally:
        client.close()


if __name__ == "__main__":
    main()
