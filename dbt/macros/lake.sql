{#- Reads a Silver table. Production scans Delta on the S3 store; CI scans parquet fixtures. -#}
{% macro lake_table(name) -%}
  {%- if target.name == 'ci' -%}
    read_parquet('{{ env_var("DBT_FIXTURES_DIR", "../tests/fixtures") }}/{{ name }}.parquet')
  {%- else -%}
    delta_scan('s3://{{ env_var("LAKE_BUCKET", "lake") }}/silver/{{ name }}')
  {%- endif -%}
{%- endmacro %}
