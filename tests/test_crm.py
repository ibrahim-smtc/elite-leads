import httpx
import pytest

from sheets_connector.settings import CRMProfile
from sheets_connector.sync import (CRMConfigError, CRMRejectedError, CRMUnavailableError, RateLimiter,
                                  RecordsClient)

PROFILE = CRMProfile("default", "https://crm.example/records", "src_key_123")


def client_for(responses, sleeps=None):
    calls = []

    def handler(request):
        calls.append(request)
        item = responses.pop(0)
        if isinstance(item, Exception):
            raise item
        status, body, headers = item if isinstance(item, tuple) else (item, {}, {})
        return httpx.Response(status, json=body, headers=headers)

    c = RecordsClient(PROFILE, http=httpx.Client(transport=httpx.MockTransport(handler)), max_requests_per_second=1000,
                      retries=3, sleep=(sleeps.append if sleeps is not None else (lambda s: None)), jitter=lambda: 0)
    return c, calls


def test_success_sends_key_and_idempotency_header():
    c, calls = client_for([(200, {"status": "created", "external_record_id": "rec_9"}, {})])
    res = c.post({"external_id": "gs-abc", "Name": "Asha"})
    assert (res.outcome, res.record_id) == ("created", "rec_9")
    assert calls[0].headers["authorization"] == "Bearer src_key_123"
    assert calls[0].headers["idempotency-key"] == "gs-abc"


def test_nested_result_and_unknown_body():
    c, _ = client_for([(200, {"result": {"outcome": "joined_existing", "id": "r1"}}, {}), (202, {}, {})])
    assert c.post({}).outcome == "joined_existing"
    assert c.post({}).outcome == "accepted"


def test_rate_limited_then_ok_honours_retry_after():
    sleeps = []
    c, calls = client_for([(429, {}, {"Retry-After": "7"}), (200, {"status": "created"}, {})], sleeps)
    assert c.post({}).outcome == "created" and len(calls) == 2 and sleeps == [7.0]


def test_server_errors_give_up_after_retries():
    sleeps = []
    c, calls = client_for([503, 502, 500, 504], sleeps)
    with pytest.raises(CRMUnavailableError):
        c.post({})
    assert len(calls) == 4 and sleeps == [1.0, 2.0, 4.0]


def test_network_error_is_retried():
    c, calls = client_for([httpx.ConnectError("down"), (200, {"status": "created"}, {})])
    assert c.post({}).outcome == "created" and len(calls) == 2


@pytest.mark.parametrize("status", [401, 403, 404])
def test_bad_key_or_address_is_a_config_error_without_retries(status):
    c, calls = client_for([status])
    with pytest.raises(CRMConfigError):
        c.post({})
    assert len(calls) == 1


@pytest.mark.parametrize("status", [400, 409, 413, 422])
def test_content_refusal_is_not_retried(status):
    c, calls = client_for([(status, {"error": {"message": "bad record"}}, {})])
    with pytest.raises(CRMRejectedError, match="bad record"):
        c.post({})
    assert len(calls) == 1


def test_rate_limiter_spaces_calls():
    t = [0.0]
    slept = []
    rl = RateLimiter(2, clock=lambda: t[0], sleep=lambda s: slept.append(s))
    rl.wait(); rl.wait(); rl.wait()
    assert slept == [0.5, 1.0]


def test_the_crms_record_status_is_kept():
    c, _ = client_for([(200, {"ok": True, "external_record_id": "r1", "status": "needs_person", "repeat": True}, {})])
    res = c.post({})
    assert res.crm_status == "needs_person" and res.record_id == "r1"
