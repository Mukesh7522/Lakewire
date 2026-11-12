"""Fail (exit 1) when the exported health snapshot is stale. Used by the publish workflow."""
from __future__ import annotations

import argparse
import json
import sys


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("health_json")
    parser.add_argument("--max-lag-minutes", type=int, default=30)
    args = parser.parse_args()
    payload = json.load(open(args.health_json))
    lag = payload.get("lag_seconds")
    if lag is None:
        print("FAIL: no latest_event_ts in health snapshot")
        return 1
    print(f"latest event is {lag / 60:.1f} minutes old (limit {args.max_lag_minutes})")
    return 0 if lag <= args.max_lag_minutes * 60 else 1


if __name__ == "__main__":
    sys.exit(main())
