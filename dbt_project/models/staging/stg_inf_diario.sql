{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    unique_key='informe_sk',
    partition_by={'field': 'dt_comptc', 'data_type': 'date', 'granularity': 'day'},
    cluster_by=['fundo_cnpj']
) }}

{#- Informe diário tipado e deduplicado. Dois schemas convivem no raw (CNPJ_FUNDO antigo e
    CNPJ_FUNDO_CLASSE novo, com ID_SUBCLASSE): first_of() pega o que existir em cada linha. -#}
{%- set src = source('raw', 'inf_diario') -%}

with src as (
    select * from {{ src }}
    {{ incremental_loaded_filter() }}
),

typed as (
    select
        {{ digits_only(first_of(src, ['cnpj_fundo_classe', 'cnpj_fundo'])) }}   as fundo_cnpj,
        {{ first_of(src, ['tp_fundo_classe', 'tp_fundo']) }}                    as tp_fundo_classe,
        coalesce({{ col_or_null(src, 'id_subclasse') }}, '')                    as id_subclasse,
        {{ dbt.safe_cast('dt_comptc', 'date') }}                                as dt_comptc,
        {{ dbt.safe_cast('vl_total', type_money()) }}                           as vl_total,
        {{ dbt.safe_cast('vl_quota', type_quota()) }}                           as vl_quota,
        {{ dbt.safe_cast('vl_patrim_liq', type_money()) }}                      as vl_patrim_liq,
        {{ dbt.safe_cast('captc_dia', type_money()) }}                          as captc_dia,
        {{ dbt.safe_cast('resg_dia', type_money()) }}                           as resg_dia,
        {{ dbt.safe_cast('nr_cotst', dbt.type_int()) }}                         as nr_cotst,
        _src_month,
        _src_row,
        _loaded_at
    from src
),

ranked as (
    select
        *,
        row_number() over (
            partition by fundo_cnpj, id_subclasse, dt_comptc
            order by _loaded_at desc, _src_row desc
        ) as rn
    from typed
)

select
    {{ dbt.hash(dbt.concat(['fundo_cnpj', "'|'", 'id_subclasse', "'|'", 'cast(dt_comptc as ' ~ dbt.type_string() ~ ')'])) }} as informe_sk,
    fundo_cnpj,
    tp_fundo_classe,
    id_subclasse,
    dt_comptc,
    vl_total,
    vl_quota,
    vl_patrim_liq,
    captc_dia,
    resg_dia,
    nr_cotst,
    _src_month,
    _loaded_at
from ranked
where rn = 1
