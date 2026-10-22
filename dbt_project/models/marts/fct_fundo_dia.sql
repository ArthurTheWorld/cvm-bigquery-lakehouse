{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    unique_key='informe_sk',
    partition_by={'field': 'dt_comptc', 'data_type': 'date', 'granularity': 'day'},
    cluster_by=['fundo_cnpj']
) }}

-- Grão: fundo/classe (+ subclasse) por dia. Partição por dia + cluster por fundo: a consulta
-- típica ("este fundo neste período") lê só as partições e blocos necessários, e é isso que
-- mantém o custo do BigQuery previsível.
select
    informe_sk,
    fundo_cnpj,
    id_subclasse,
    tp_fundo_classe,
    dt_comptc,
    vl_quota,
    vl_patrim_liq,
    vl_total,
    coalesce(captc_dia, 0)                          as captacao,
    coalesce(resg_dia, 0)                           as resgate,
    coalesce(captc_dia, 0) - coalesce(resg_dia, 0)  as captacao_liquida,
    nr_cotst                                        as cotistas,
    _loaded_at
from {{ ref('stg_inf_diario') }}
{{ incremental_loaded_filter() }}
