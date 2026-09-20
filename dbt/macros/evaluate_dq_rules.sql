-- evaluate_dq_rules.sql
-- Reusable macro that evaluates a list of data quality rules and generates:
--   dq_failed_rules  — ARRAY<STRING> of rule names where the check failed
--   is_quarantined   — BOOLEAN, true if any rule failed
--
-- Each rule is a dict with:
--   name: human-readable rule identifier
--   expr: SQL boolean expression that returns TRUE when the rule PASSES
--
-- Usage:
--   {{ evaluate_dq_rules([
--       {'name': 'id_not_null', 'expr': 'id IS NOT NULL'},
--       {'name': 'count_positive', 'expr': 'count >= 0'},
--   ]) }}

{% macro evaluate_dq_rules(rules) %}
    array_compact(array(
        {%- for rule in rules %}
        CASE WHEN ({{ rule.expr }}) THEN NULL ELSE '{{ rule.name }}' END
        {%- if not loop.last %},{% endif %}
        {%- endfor %}
    )) as dq_failed_rules,
    size(array_compact(array(
        {%- for rule in rules %}
        CASE WHEN ({{ rule.expr }}) THEN NULL ELSE '{{ rule.name }}' END
        {%- if not loop.last %},{% endif %}
        {%- endfor %}
    ))) > 0 as is_quarantined
{% endmacro %}
