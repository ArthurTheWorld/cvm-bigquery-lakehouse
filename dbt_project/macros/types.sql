{#- Tipos que o dbt não cobre.
    BigQuery: NUMERIC tem só 9 casas decimais e a cota do fundo (VL_QUOTA) vem com 12;
    NUMERIC TRUNCARIA. Por isso a cota é BIGNUMERIC (até 38 casas). -#}
{% macro type_quota() -%} {{ return(adapter.dispatch('type_quota', 'cvm_fundos')()) }} {%- endmacro %}
{% macro default__type_quota() -%} decimal(38, 12) {%- endmacro %}
{% macro bigquery__type_quota() -%} bignumeric {%- endmacro %}

{% macro type_money() -%} {{ return(adapter.dispatch('type_money', 'cvm_fundos')()) }} {%- endmacro %}
{% macro default__type_money() -%} decimal(18, 2) {%- endmacro %}
{% macro bigquery__type_money() -%} numeric {%- endmacro %}

{#- Ponto flutuante de 64 bits nos dois bancos. dbt.type_float() NÃO serve: no DuckDB é FLOAT de
    32 bits (~7 dígitos), no BigQuery é FLOAT64. -#}
{% macro type_double() -%} {{ return(adapter.dispatch('type_double', 'cvm_fundos')()) }} {%- endmacro %}
{% macro default__type_double() -%} double {%- endmacro %}
{% macro bigquery__type_double() -%} float64 {%- endmacro %}
