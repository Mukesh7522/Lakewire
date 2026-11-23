# Drills 2 and 3: poison messages, a schema change, and a real false alarm

**Type:** planned failure drill that also found a real bug. **Date:** 2026-11-23, all times UTC.

## What we tested

1. A malformed event must land in the dead-letter queue (DLQ) without stopping the stream.
2. A new upstream field must pass; a changed field type must be rejected.

## How

Six crafted events were replayed through the same code path as the live producer
(`python -m lakewire.producer --replay`), while the live producer kept running. The consumer was down at the time
(see the consumer outage drill), which shows the DLQ does not depend on Spark.

| Event | Expected | Actual |
|---|---|---|
| Not valid JSON | DLQ | DLQ: `Expecting property name enclosed in double quotes` |
| A JSON array, not an object | DLQ | DLQ: `event is not a JSON object` |
| Missing the required `user` field | DLQ | DLQ: `'user' is a required property` |
| `namespace` changed from integer to the string `"main"` | DLQ | DLQ: `namespace: 'main' is not of type 'integer'` |
| Valid event with an extra field `new_upstream_field` | passes | passed |
| Event type `log` (not an edit) | skipped | skipped |

Replay result: `ok 1, skip 1, dlq 4`. The DLQ topic went from 2 to 6 messages. The live producer's last-event age stayed at
0.9 seconds. After the consumer restarted, exactly one drill event (the additive-field one, user `lakewire-drill`) was in
Silver and none of the four rejected ones were.

## The finding: Wikimedia's canary events were being dead-lettered

The DLQ already held 2 messages before the drill. They were real: payloads with only `$schema` and `meta`, where
`meta.domain` is `canary`. Wikimedia emits these synthetic events to test its own pipelines, two per hour at :15 (one per
datacenter topic). Two more arrived at 07:15 while the drill was running.

They are not bad data, but the contract treated them as violations. In production that means a steady trickle of
dead letters and a permanent false "dead-letter rate rising" signal, which trains people to ignore the real one.

## Fix

`classify()` in `lakewire/contract.py` now skips events whose `meta.domain` is `canary` before validation. A regression
test (`test_wikimedia_canary_events_are_skipped_not_dead_lettered`) also checks that the same payload without the canary
marker still goes to the DLQ. Deployed to the live producer at 07:24:50.

## Verification of the fix

The two canary messages that arrived at 07:15 (DLQ offsets 6 and 7, `meta.dt` 07:15:00, domain `canary`) were read back
out of the DLQ and replayed through the deployed producer image at 07:26: `skip 2, dlq 0`, and the DLQ end offset stayed
at 8. Live confirmation: the 08:15 canaries arrived while the laptop was asleep (see the laptop-sleep report), so the producer
received them in its wake-up backfill. The DLQ end offset was still 8 at 08:56 and again at 09:03, so they were skipped,
not dead-lettered.

## Prevention

Treat every stream-level control message as a first-class case in the contract, and keep the DLQ clean enough that any
new message in it is worth a look.
