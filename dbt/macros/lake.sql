{#- Reads a Silver table. Production scans Delta on the S3 store; CI scans parquet fixtures. -#}
{#- The pageviews table is written only by the daily Airflow DAG, so on a fresh deploy it does not exist
    for hours. Rather than fail every hourly marts run, treat a missing table as empty. -#}
{% macro lake_table(name) -%}
  {%- if target.name == 'ci' -%}
    read_parquet('{{ env_var("DBT_FIXTURES_DIR", "../tests/fixtures") }}/{{ name }}.parquet')
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
