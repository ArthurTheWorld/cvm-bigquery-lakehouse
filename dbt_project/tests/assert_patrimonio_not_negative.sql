{{ config(severity='warn') }}

select informe_sk, fundo_cnpj, dt_comptc, vl_patrim_liq
from {{ ref('stg_inf_diario') }}
where vl_patrim_liq < 0
