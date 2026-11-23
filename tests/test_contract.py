import json
from pathlib import Path

from lakewire.contract import classify

WIKIS = ("enwiki", "dewiki")
FIXTURE = Path(__file__).parent / "fixtures" / "sample_events.jsonl"


def good():
    return {
        "meta": {"id": "e1", "dt": "2026-11-10T08:00:00Z"},
        "type": "edit", "wiki": "enwiki", "title": "Cats", "user": "Ann", "namespace": 0,
        "bot": False, "length": {"old": 10, "new": 20}, "revision": {"old": 1, "new": 2},
    }


def test_valid_event_is_normalised():
    outcome, row = classify(good(), WIKIS)
    assert outcome == "ok"
    assert row["event_id"] == "e1" and row["length_new"] == 20 and row["is_bot"] is False


def test_new_page_without_old_length_is_valid():
    ev = good()
    ev["type"] = "new"
    ev["length"] = {"new": 5}
    outcome, row = classify(ev, WIKIS)
    assert outcome == "ok" and row["length_old"] is None


def test_log_events_and_other_wikis_are_skipped():
    ev = good()
    ev["type"] = "log"
    assert classify(ev, WIKIS)[0] == "skip"
    ev = good()
    ev["wiki"] = "wikidatawiki"
    assert classify(ev, WIKIS)[0] == "skip"


def test_missing_required_field_goes_to_dlq():
    ev = good()
    del ev["meta"]["id"]
    outcome, errors = classify(ev, WIKIS)
    assert outcome == "dlq" and any("id" in e for e in errors)


def test_breaking_type_change_goes_to_dlq_but_additive_field_passes():
    ev = good()
    ev["namespace"] = "zero"  # breaking: integer became string
    assert classify(ev, WIKIS)[0] == "dlq"
    ev = good()
    ev["brand_new_field"] = {"anything": 1}  # additive: must flow through
    assert classify(ev, WIKIS)[0] == "ok"


def test_missing_type_is_not_silently_skipped():
    ev = good()
    del ev["type"]
    assert classify(ev, WIKIS)[0] == "dlq"


def test_fixture_matches_expected_counts():
    expected = json.loads((FIXTURE.parent / "expected.json").read_text())
    counts = {"ok": 0, "skip": 0, "dlq": 0}
    for line in FIXTURE.read_text().splitlines():
        try:
            raw = json.loads(line)
        except json.JSONDecodeError:
            counts["dlq"] += 1
            continue
        counts[classify(raw, ("enwiki", "dewiki", "frwiki", "eswiki", "jawiki"))[0]] += 1
    assert counts == {"ok": expected["producer_ok"], "skip": expected["producer_skip"], "dlq": expected["producer_dlq"]}


def test_wikimedia_canary_events_are_skipped_not_dead_lettered():
    from lakewire.contract import classify

    canary = {"$schema": "/mediawiki/recentchange/1.0.1",
              "meta": {"dt": "2026-11-23T06:15:00.000Z", "domain": "canary", "id": "70d2479c", "stream": "x"}}
    assert classify(canary, ("enwiki",)) == ("skip", None)
    # The same payload without the canary marker really is a contract violation.
    broken = {**canary, "meta": {**canary["meta"], "domain": "en.wikipedia.org"}}
    assert classify(broken, ("enwiki",))[0] == "dlq"
