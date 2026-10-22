{{ config(severity='warn') }}

-- Fundos que informam cota mas não estão no cadastro (um por linha). Aparecem como 'SEM
-- CADASTRO' em agg_mercado_mensal e ficam de fora de agg_gestora_mensal.
select distinct i.fundo_cnpj
from {{ ref('stg_inf_diario') }} as i
left join {{ ref('stg_cad_fi') }} as c on c.fundo_cnpj = i.fundo_cnpj
where c.fundo_cnpj is null
