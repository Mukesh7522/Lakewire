{{ config(materialized='incremental', unique_key='row_key', incremental_strategy='delete+insert') }}

-- Heuristic signals for contested pages. Not a verdict on any editor or article.
with edits as (
    select
        wiki, title, event_date, event_ts, user_name, length_old, length_new,
        lag(user_name) over (partition by wiki, title order by event_ts) as prev_user,
        lag(event_ts)  over (partition by wiki, title order by event_ts) as prev_ts
    from {{ ref('stg_edits') }}
    where event_type = 'edit'
    {% if is_incremental() %}
      and event_date >= (select coalesce(max(event_date), date '1970-01-01') - interval 1 day from {{ this }})
    {% endif %}
),
daily as (
    select
        wiki, title, event_date,
        count(*) as edits,
        count(distinct user_name) as distinct_editors,
        count(*) filter (
            where prev_user is not null and prev_user <> user_name
              and date_diff('second', prev_ts, event_ts) <= 600
        ) as rapid_alternations,
        sum(abs(coalesce(length_new, 0) - coalesce(length_old, 0))) as bytes_swing
    from edits
    group by wiki, title, event_date
)
select
    wiki || '|' || title || '|' || event_date::varchar as row_key,
    *,
    rapid_alternations * least(distinct_editors, 5) as conflict_score,
    (rapid_alternations >= 3 and distinct_editors >= 2) as is_flagged
from daily
where edits >= 3
