{{ config(severity='warn') }}

-- Cota <= 0 é defeito de dado (a cota de um fundo não zera nem fica negativa): fica visível
-- em toda execução sem derrubar o pipeline por algo que a fonte é quem tem de corrigir.
select informe_sk, fundo_cnpj, dt_comptc, vl_quota
from {{ ref('stg_inf_diario') }}
where vl_quota <= 0
