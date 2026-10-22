{#- Filtro incremental por _loaded_at com janela de segurança.

    Por que _loaded_at e não _src_month ou dt_comptc: a CVM regrava o arquivo do mês
    corrente todo dia útil e aceita REAPRESENTAÇÕES de dias anteriores, inclusive de meses
    já fechados. Cada recarga do raw dá um _loaded_at novo a TODAS as linhas daquele mês;
    filtrar por "mês >= o último que já vi" perderia correções de meses anteriores.
    A janela (lookback) cobre cargas que terminam fora de ordem. Reprocessar é seguro:
    insert_overwrite substitui partições inteiras e a deduplicação mantém a última linha. -#}
{#- O resultado precisa ser TIMESTAMP (o tipo de _loaded_at). dbt.dateadd NÃO serve: no BigQuery
    ele devolve DATETIME, e `TIMESTAMP > DATETIME` é erro de tipo. -#}
{% macro loaded_at_watermark(column, hours) -%}
    {{ return(adapter.dispatch('loaded_at_watermark', 'cvm_fundos')(column, hours)) }}
{%- endmacro %}
{% macro default__loaded_at_watermark(column, hours) -%}
    coalesce(max({{ column }}), cast('1900-01-01' as timestamp)) - interval '{{ hours }} hour'
{%- endmacro %}
{% macro bigquery__loaded_at_watermark(column, hours) -%}
    timestamp_sub(coalesce(max({{ column }}), timestamp '1900-01-01'), interval {{ hours }} hour)
{%- endmacro %}

{% macro incremental_loaded_filter(column='_loaded_at') %}
    {% if is_incremental() %}
    where {{ column }} > (
        select {{ loaded_at_watermark(column, var('incremental_lookback_hours', 6) | int) }}
        from {{ this }}
    )
    {% endif %}
{% endmacro %}
