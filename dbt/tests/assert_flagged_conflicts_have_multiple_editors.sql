select * from {{ ref('mart_edit_conflict_signals') }}
where is_flagged and distinct_editors < 2
