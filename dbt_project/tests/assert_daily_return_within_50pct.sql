{{ config(severity='warn') }}

-- Variação diária de cota acima de +-50% quase sempre é erro de digitação na fonte (ou split
-- não ajustado). Só compara cotas positivas consecutivas da MESMA série (fundo + subclasse).
with series as (
    select
        fundo_cnpj, id_subclasse, dt_comptc, vl_quota,
        lag(vl_quota) over (partition by fundo_cnpj, id_subclasse order by dt_comptc) as quota_anterior
    from {{ ref('stg_inf_diario') }}
)

select fundo_cnpj, id_subclasse, dt_comptc, vl_quota, quota_anterior
from series
where vl_quota > 0
  and quota_anterior > 0
  -- nullif: o BigQuery não garante curto-circuito no AND, e há cotas iguais a zero
  and abs(vl_quota / nullif(quota_anterior, 0) - 1) > 0.5
