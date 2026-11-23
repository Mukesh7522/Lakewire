import importlib.util
from pathlib import Path

import pytest

_script = Path(__file__).resolve().parent.parent / "scripts" / "week_report.py"
_spec = importlib.util.spec_from_file_location("week_report", _script)
week_report = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(week_report)


def test_parse_mem_mib_reads_the_used_part_in_mib():
    assert week_report.parse_mem_mib("2GiB / 4GiB") == 2048
    assert week_report.parse_mem_mib("512MiB / 1GiB") == 512
    assert week_report.parse_mem_mib("1024KiB / 1GiB") == 1
    with pytest.raises(ValueError):
        week_report.parse_mem_mib("lots")


def test_coverage_is_the_share_of_the_window_with_samples():
    assert week_report.coverage(240, 3600) == 1.0   # 240 samples x 15 s = one hour
    assert week_report.coverage(120, 3600) == 0.5
    assert week_report.coverage(500, 3600) == 1.0   # never above 100%
    assert week_report.coverage(10, 0) == 0.0


def test_find_gaps_returns_only_holes_longer_than_the_limit():
    stamps = [0, 60, 120, 3720, 3780]
    assert week_report.find_gaps(stamps, 180) == [(120, 3600)]
    assert week_report.find_gaps([0, 60, 120], 180) == []


def test_fmt_duration_picks_a_readable_unit():
    assert week_report.fmt_duration(300) == "5 min"
    assert week_report.fmt_duration(7200) == "2.0 h"
    assert week_report.fmt_duration(3 * 86400) == "3.0 days"
