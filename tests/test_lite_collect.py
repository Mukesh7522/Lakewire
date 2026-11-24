from datetime import datetime, timedelta

from lite_helpers import BULK, history_two_hours_ago, make_event, naive_now, serve, settings_for

from lakewire.lite import collect as lite_collect


def test_collect_keeps_only_wanted_wikis_and_stops_once_caught_up(tmp_path):
    server, seen = serve(history_two_hours_ago())
    result = lite_collect.collect(settings_for(server), tmp_path, naive_now(), grace_seconds=0.3, max_seconds=30)
    server.shutdown()
    assert result.caught_up and not result.timed_out
    assert result.bulk_topic == BULK  # found from the event counts, not a hard-coded data-centre name
    assert {e["wiki"] for e in result.events} == {"enwiki", "dewiki"}  # commonswiki was filtered out
    assert result.filtered >= 100 and result.read > 300
    assert result.first_bulk_dt < naive_now() - timedelta(hours=1)  # the replay started in the past
    assert "since=" in seen["paths"][0] and seen["headers"][0] is None  # first run: since, no saved position


def test_next_run_resumes_from_the_saved_stream_position(tmp_path):
    server, seen = serve(history_two_hours_ago())
    lite_collect.collect(settings_for(server), tmp_path, naive_now(), grace_seconds=0.2, max_seconds=30)
    saved = (tmp_path / "last_event_id").read_text().strip()
    assert saved != ""
    second = lite_collect.collect(settings_for(server), tmp_path, naive_now(), grace_seconds=0.2, max_seconds=30,
                                  min_events_for_live=50)
    server.shutdown()
    assert second.resumed
    assert saved in {h for h in seen["headers"] if h}  # the second run asked to continue from the saved position
    assert second.read < 150  # it did not replay the 300-event history again


def test_time_budget_stops_a_run_that_never_catches_up(tmp_path):
    old = naive_now() - timedelta(hours=5)
    history = [make_event(i, old + timedelta(seconds=i)) for i in range(400)]
    server, _ = serve(history)
    # Pretend the run started in the future, so the fake "live" events never count as caught up.
    result = lite_collect.collect(settings_for(server), tmp_path, naive_now() + timedelta(days=2), max_seconds=1,
                                  grace_seconds=100, live_slack_seconds=1)
    server.shutdown()
    assert result.timed_out and not result.caught_up


def test_contract_violations_are_dead_lettered_not_kept(tmp_path):
    start = naive_now() - timedelta(minutes=30)
    history = [make_event(i, start + timedelta(seconds=i)) for i in range(250)]
    history.insert(10, make_event(777, start, namespace="main"))  # wrong type
    history.insert(11, make_event(778, start, user=None))          # missing required value
    server, _ = serve(history)
    result = lite_collect.collect(settings_for(server), tmp_path, naive_now(), grace_seconds=0.2, max_seconds=30)
    server.shutdown()
    assert len(result.dlq) >= 2
    assert all(e["event_id"] not in {"evt-777", "evt-778"} for e in result.events)


def test_gap_minutes_flags_a_run_that_started_long_after_the_previous_one_ended():
    previous = datetime(2026, 11, 23, 10, 0)
    assert lite_collect.gap_minutes(previous, datetime(2026, 11, 23, 10, 2)) == 2
    assert lite_collect.gap_minutes(previous, datetime(2026, 11, 23, 12, 0)) == 120
    assert lite_collect.gap_minutes(None, previous) is None
