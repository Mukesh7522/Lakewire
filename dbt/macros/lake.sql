{#- Reads a Silver table. Production scans Delta on the S3 store; CI scans parquet fixtures; the lightweight GitHub
    Actions pipeline ("lite") scans the Parquet files it keeps in the data branch. -#}
{#- The pageviews table is written only by the daily job, so on a fresh deploy it does not exist for hours. Rather
    than fail every hourly marts run, treat a missing table as empty. -#}
{% macro lake_table(name) -%}
  {%- if target.name == 'ci' -%}
    read_parquet('{{ env_var("DBT_FIXTURES_DIR", "../tests/fixtures") }}/{{ name }}.parquet')
  {%- elif target.name == 'lite' -%}
    {%- set pattern = env_var("LITE_DATA_DIR", "lake") ~ '/silver/' ~ name ~ '/*/*.parquet' -%}
    {%- set found = 1 -%}
    {%- if execute -%}
      {%- set found = run_query("select count(*) from glob('" ~ pattern ~ "')").columns[0].values()[0] -%}
    {%- endif -%}
    {%- if found > 0 -%}
      read_parquet('{{ pattern }}', union_by_name=true)
    {%- else -%}
      {{ lite_empty(name) }}
    {%- endif -%}
  {%- else -%}
    {%- set path = 's3://' ~ env_var("LAKE_BUCKET", "lake") ~ '/silver/' ~ name -%}
    {%- set found = 1 -%}
    {%- if execute and name == 'pageviews' -%}
      {%- set found = run_query("select count(*) from glob('" ~ path ~ "/_delta_log/*.json')").columns[0].values()[0] -%}
    {%- endif -%}
    {%- if found > 0 -%}
      delta_scan('{{ path }}')
    {%- else -%}
      (select null::timestamp as view_hour, null::varchar as page_title, null::bigint as views where false)
    {%- endif -%}
  {%- endif -%}
{%- endmacro %}

{#- A typed, empty stand-in so the models still compile before the first files exist. -#}
{% macro lite_empty(name) -%}
  {%- if name == 'edits' -%}
    (select null::varchar as event_id, null::timestamp as event_ts, null::date as event_date, null::varchar as wiki,
            null::varchar as event_type, null::varchar as title, null::bigint as namespace, null::varchar as "user",
            null::boolean as is_bot, null::boolean as is_minor, null::bigint as length_old, null::bigint as length_new,
            null::bigint as rev_old, null::bigint as rev_new, null::timestamp as ingested_at where false)
  {%- else -%}
    (select null::timestamp as view_hour, null::date as view_date, null::varchar as domain_code,
            null::varchar as page_title, null::bigint as views where false)
  {%- endif -%}
{%- endmacro %}
