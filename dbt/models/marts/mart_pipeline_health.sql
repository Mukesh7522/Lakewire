-- One row describing freshness and volume. The dashboard and CI health check read this.
select
    current_timestamp as built_at,
    max(event_ts) as latest_event_ts,
    count(*) filter (where event_ts >= current_timestamp::timestamp - interval 1 hour) as events_last_hour,
    count(*) filter (where event_ts >= current_timestamp::timestamp - interval 24 hour) as events_last_24h,
    count(*) as events_total,
    count(distinct wiki) as wikis
from {{ ref('stg_edits') }}
