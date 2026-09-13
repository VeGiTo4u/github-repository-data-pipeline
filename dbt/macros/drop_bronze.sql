{% macro drop_bronze_tables() %}
    {% set tables = ['repositories', 'issues', 'pull_requests', 'releases', 'languages'] %}
    
    {% for table_name in tables %}
        {% set query %}
            DROP TABLE IF EXISTS github_analytics.bronze.{{ table_name }};
        {% endset %}
        
        {% do log("Dropping table: " ~ table_name, info=True) %}
        {% do run_query(query) %}
    {% endfor %}
    
    {% do log("All bronze tables dropped successfully.", info=True) %}
{% endmacro %}
