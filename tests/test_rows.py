import pytest

from conftest import REVSPOT_HEADERS, SHEET_ID, revspot_row
from sheets_connector.sheets import PlanError, clean_value, mask, normalize_headers, plan_rows
from sheets_connector.sheets import parse_sheet_url


def plan(values, **kw):
    return plan_rows(values, spreadsheet_id=SHEET_ID, gid="0", **kw)


def test_parse_sheet_url():
    ref = parse_sheet_url(f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/edit?gid=0#gid=123")
    assert ref.spreadsheet_id == SHEET_ID and ref.gid == "0"
    assert parse_sheet_url(f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/edit").gid is None
    with pytest.raises(ValueError):
        parse_sheet_url("https://example.com/not-a-sheet")


def test_clean_values_and_headers():
    assert clean_value("  NA ") is None and clean_value("") is None and clean_value("  a   b ") == "a b"
    assert normalize_headers(["Name", "", "name", "Phone"], 5) == ["Name", "Column B", "name (2)", "Phone", "Column E"]


def test_revspot_layout_is_keyed_by_phone_and_date():
    p = plan([REVSPOT_HEADERS, revspot_row("Jason", "918904371882", "jason@x.com")])
    assert p.id_strategy == "contact"
    assert set(p.contact_columns) == {"Phone", "Email"}
    row = p.rows[0]
    assert row.has_contact and row.keyed_by == "contact" and row.row_number == 2
    assert row.payload["Name"] == "Jason" and row.payload["Preferred Model"] == "Virtus GT"
    assert "Current Status" not in row.payload            # empty cells are not sent
    assert row.payload["external_id"] == row.external_id and row.payload["_source"]["system"] == "google_sheets"


def test_any_layout_with_an_id_column():
    p = plan([["Lead ID", "Customer", "Mobile No", "Car"], ["L-1", "Asha", "98450 12345", "Taigun"],
              ["L-2", "Ravi", "", "Virtus"]])
    assert p.id_strategy == "id" and p.id_columns == ["Lead ID"]
    assert [r.has_contact for r in p.rows] == [True, False]


def test_layout_without_id_or_contact_warns():
    p = plan([["Product", "Qty"], ["Virtus", "2"]])
    assert p.id_strategy == "content" and p.warnings


def test_explicit_id_columns_must_exist():
    with pytest.raises(PlanError):
        plan([["A", "B"], ["1", "2"]], id_columns=["Nope"])


def test_edit_changes_hash_but_not_key():
    a = plan([REVSPOT_HEADERS, revspot_row("Jason", "918904371882", model="Virtus GT")]).rows[0]
    b = plan([REVSPOT_HEADERS, revspot_row("Jason", "918904371882", model="Taigun")]).rows[0]
    assert a.key_hash == b.key_hash and a.content_hash != b.content_hash and a.external_id != b.external_id


def test_row_moving_down_keeps_its_key():
    a = plan([REVSPOT_HEADERS, revspot_row("Jason", "918904371882")]).rows[0]
    b = plan([REVSPOT_HEADERS, revspot_row("Asha", "919845012345"), revspot_row("Jason", "918904371882")]).rows[1]
    assert a.key_hash == b.key_hash and a.content_hash == b.content_hash and b.row_number == 3


def test_ignored_columns_do_not_change_the_hash():
    a = plan([REVSPOT_HEADERS, revspot_row("Jason", "918904371882", status="")], ignore_columns=["Most Recent Status", "Attempt #"]).rows[0]
    b = plan([REVSPOT_HEADERS, revspot_row("Jason", "918904371882", status="did not pick", attempt="1")],
             ignore_columns=["Most Recent Status", "Attempt #"]).rows[0]
    assert a.content_hash == b.content_hash
    assert b.payload["Most Recent Status"] == "did not pick"      # still sent when the row goes out


def test_duplicate_keys_keep_both_rows():
    p = plan([["Phone", "Note"], ["9845012345", "first"], ["9845012345", "second"]])
    assert len({r.key_hash for r in p.rows}) == 2


def test_empty_rows_skipped_and_ragged_rows_padded():
    p = plan([["Name", "Phone"], [], ["", ""], ["Asha", "9845012345", "extra"]])
    assert len(p.rows) == 1 and p.headers == ["Name", "Phone", "Column C"]
    assert p.rows[0].payload["Column C"] == "extra"


def test_reserved_names_extra_fields_and_form_key():
    p = plan([["Name", "Phone", "external_id"], ["Asha", "9845012345", "theirs"]],
             extra_fields={"Lead source": "Revspot", "Name": "never overrides"}, form_key="vw-blr")
    payload = p.rows[0].payload
    assert payload["external_id (sheet)"] == "theirs" and payload["external_id"].startswith("gs-")
    assert payload["Lead source"] == "Revspot" and payload["Name"] == "Asha" and payload["form_key"] == "vw-blr"


def test_last_updated_is_not_used_as_the_key_date():
    headers = ["Last Updated", "Phone", "Created"]
    a = plan([headers, ["2026-11-01", "9845012345", "2026-10-01"]]).rows[0]
    b = plan([headers, ["2026-11-05", "9845012345", "2026-10-01"]]).rows[0]
    assert a.key_hash == b.key_hash


def test_mask():
    assert mask("jason@gmail.com") == "j***@gmail.com"
    assert mask("+91 98450 12345").endswith("345") and "9845" not in mask("+91 98450 12345")
    assert mask("2026-11-12") == "2026-11-12" and mask("Virtus GT") == "Virtus GT"
    assert mask({"a": "x@y.com"}) == {"a": "x***@y.com"}


def test_phone_found_by_value_whatever_the_column_is_called():
    p = plan([["Client's Name", "WA no", "Car"], ["Asha", "+91 98450 12345", "Taigun"], ["Ravi", "9845012346", "Virtus"]])
    assert p.contact_columns == ["WA no"] and all(r.has_contact for r in p.rows) and p.id_strategy == "contact"


def test_ids_dates_and_amounts_are_not_mistaken_for_phones():
    p = plan([["Name", "Order", "Created", "Amount"], ["Asha", "100200300400", "2026-11-12", "2500000"]])
    assert p.contact_columns == [] and not p.rows[0].has_contact
    p = plan([["Call ID", "Name", "Ref"], ["6359a01d-407d", "Asha", "9876543210123456"]])
    assert p.contact_columns == []
