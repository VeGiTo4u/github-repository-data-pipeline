-- create_silver_external_table.sql
-- Post-hook macro that registers a silver Delta table as an external table
-- in Unity Catalog, backed by S3 under the silver/ prefix.
-- Mirrors the Bronze pattern: data lives in S3, Databricks reads via external location.
--
-- Usage in dbt_project.yml:
--   +post-hook: "{{ create_silver_external_table() }}"

{% macro create_silver_external_table() %}
    {%- set s3_bucket = env_var('S3_BUCKET_NAME') -%}
    {%- set table_name = this.name -%}
    {%- set s3_path = 's3://' ~ s3_bucket ~ '/silver/' ~ table_name -%}
    {%- set full_table = this.database ~ '.' ~ this.schema ~ '.' ~ table_name -%}

    CREATE TABLE IF NOT EXISTS {{ full_table }}
    USING DELTA
    LOCATION '{{ s3_path }}'
{% endmacro %}
