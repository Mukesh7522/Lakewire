select
    event_id,
    event_ts::timestamp as event_ts,
    event_ts::date as event_date,
    wiki,
    event_type,
    title,
    namespace,
    "user" as user_name,
    coalesce(is_bot, false) as is_bot,
    coalesce(is_minor, false) as is_minor,
    length_old,
    length_new
from {{ lake_table('edits') }}
