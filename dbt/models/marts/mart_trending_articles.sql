{{ config(materialized='incremental', unique_key='row_key', incremental_strategy='delete+insert') }}

-- Trend score = log(1 + views) * growth versus the same hour on the previous day (1.0 if unknown).
with views as (
    select view_hour, page_title, sum(views) as views
    from {{ ref('stg_pageviews') }}
    -- Articles only: the main page and namespaced pages (Special:Search, Wikipedia:..., Talk:...) dominate raw counts.
    where page_title <> 'Main_Page'
      and not regexp_matches(page_title, '^(Special|Wikipedia|Wikipedia_talk|Talk|User|User_talk|File|File_talk|Category|Category_talk|Template|Template_talk|Help|Portal|Draft|MediaWiki|Module|Book|TimedText):')
    group by view_hour, page_title
),
edits as (
    select
        date_trunc('hour', event_ts) as view_hour,
        replace(title, ' ', '_') as page_title,
        count(*) as edits_1h
    from {{ ref('stg_edits') }}
    where wiki = 'enwiki' and namespace = 0
    group by 1, 2
),
joined as (
    select
        v.view_hour, v.page_title, v.views,
        p.views as views_prev_day,
        coalesce(e.edits_1h, 0) as edits_1h
    from views v
    left join views p on p.page_title = v.page_title and p.view_hour = v.view_hour - interval 1 day
    left join edits e on e.page_title = v.page_title and e.view_hour = v.view_hour
    {% if is_incremental() %}
    where v.view_hour >= (select coalesce(max(view_hour), timestamp '1970-01-01') - interval 3 hour from {{ this }})
    {% endif %}
)
select
    page_title || '|' || strftime(view_hour, '%Y-%m-%dT%H') as row_key,
    *,
    round(views::double / nullif(views_prev_day, 0), 3) as growth_vs_prev_day,
    round(ln(1 + views) * coalesce(views::double / nullif(views_prev_day, 0), 1.0), 3) as trend_score
from joined
