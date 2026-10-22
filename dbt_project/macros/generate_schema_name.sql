{#- Usa o nome do dataset customizado (staging, marts) exatamente como está, com um
    prefixo opcional (CI usa dbt_ci_<run_id>_ para não tocar nos datasets reais). -#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set base = (custom_schema_name | trim) if custom_schema_name is not none else target.schema -%}
    {{ var('schema_prefix', '') ~ base }}
{%- endmacro %}
