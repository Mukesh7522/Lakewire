select * from {{ ref('mart_bot_vs_human') }}
where bot_share < 0 or bot_share > 1 or bot_edits + human_edits <> edits_total
