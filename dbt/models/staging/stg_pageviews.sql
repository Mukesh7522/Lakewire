select
    view_hour::timestamp as view_hour,
    view_hour::date as view_date,
    page_title,
    views
from {{ lake_table('pageviews') }}
