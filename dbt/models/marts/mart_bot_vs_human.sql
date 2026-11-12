{{ config(materialized='incremental', unique_key='row_key', incremental_strategy='delete+insert') }}

with edits as (
    select * from {{ ref('stg_edits') }}
    {% if is_incremental() %}
    -- reprocess the last two hours so late events are folded in
    where event_ts >= (select coalesce(max(hour_ts), timestamp '1970-01-01') - interval 2 hour from {{ this }})
    {% endif %}
)
select
    wiki || '|' || strftime(date_trunc('hour', event_ts), '%Y-%m-%dT%H') as row_key,
    wiki,
    date_trunc('hour', event_ts) as hour_ts,
    count(*) as edits_total,
    count(*) filter (where is_bot) as bot_edits,
    count(*) filter (where not is_bot) as human_edits,
    round(count(*) filter (where is_bot)::double / count(*), 4) as bot_share,
    round(avg(abs(coalesce(length_new, 0) - coalesce(length_old, 0))), 1) as avg_abs_size_change
from edits
group by wiki, date_trunc('hour', event_ts)
