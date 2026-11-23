import pytest

from lakewire.streaming.retry import is_concurrent_conflict, retry_on_conflict


class ConcurrentAppendException(Exception):  # same name Delta's Python exception has
    pass


def test_conflict_detection_by_class_name_and_by_error_code():
    assert is_concurrent_conflict(ConcurrentAppendException("x"))
    assert is_concurrent_conflict(RuntimeError("[DELTA_CONCURRENT_APPEND] Files were added"))
    assert not is_concurrent_conflict(ValueError("table does not exist"))


def test_retries_a_conflict_then_succeeds():
    calls, sleeps = [], []

    def flaky():
        calls.append(1)
        if len(calls) < 3:
            raise ConcurrentAppendException("lost the race")
        return "done"

    assert retry_on_conflict(flaky, attempts=5, sleep=sleeps.append) == "done"
    assert len(calls) == 3 and sleeps == [1.0, 2.0]


def test_gives_up_after_the_last_attempt():
    calls = []

    def always():
        calls.append(1)
        raise ConcurrentAppendException("still racing")

    with pytest.raises(ConcurrentAppendException):
        retry_on_conflict(always, attempts=3, sleep=lambda _s: None)
    assert len(calls) == 3


def test_other_errors_are_not_retried():
    calls = []

    def broken():
        calls.append(1)
        raise ValueError("a real bug")

    with pytest.raises(ValueError):
        retry_on_conflict(broken, attempts=5, sleep=lambda _s: None)
    assert len(calls) == 1
