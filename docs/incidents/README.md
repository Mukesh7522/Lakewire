# Incident reports

One page per incident or failure drill: what happened, the evidence, the cause, the fix, and what it still does not cover.
Times are UTC.

| Date | Report | Kind | Outcome |
|---|---|---|---|
| 2026-11-23 | [Silver stream crashed during compaction](2026-11-23-silver-compaction-crash.md) | real incident | reproduced, fixed with commit-conflict retry |
| 2026-11-23 | [Consumer down for 13 minutes](2026-11-23-consumer-outage.md) | drill | no gaps, no duplicates |
| 2026-11-23 | [Poison messages and a schema change](2026-11-23-poison-messages-and-schema-change.md) | drill | contract held; found and fixed a canary-event false alarm |
| 2026-11-23 | [Delete a pageview day and rebuild it](2026-11-23-pageview-day-rebuild.md) | drill | rebuilt day identical (rows, views, fingerprint) |

`scripts/lake_audit.py` produces the evidence used in these reports and can be rerun at any time.
