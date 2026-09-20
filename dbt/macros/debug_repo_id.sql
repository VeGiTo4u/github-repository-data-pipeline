{% macro debug_repo_id() %}
  {% set query %}
    select full_name, _repo_full_name from {{ ref('stg_github_repositories') }} where full_name like '%react%'
  {% endset %}
  {% set results = run_query(query) %}
  {% do results.print_table() %}
{% endmacro %}
