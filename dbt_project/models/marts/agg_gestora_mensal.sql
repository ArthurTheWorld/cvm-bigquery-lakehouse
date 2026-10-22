-- Patrimônio e fluxo por gestor e mês (só fundos com cadastro: o gestor vem do cadastro).
select
    m.mes,
    d.gestor_chave,
    max(d.gestor_nome)                              as gestor_nome,
    count(distinct m.fundo_cnpj)                    as fundos,
    sum(m.pl_final)                                 as pl_total,
    sum(m.captacao_liquida_mes)                     as captacao_liquida
from {{ ref('agg_fundo_mensal') }} as m
inner join {{ ref('dim_fundos') }} as d on d.fundo_cnpj = m.fundo_cnpj
group by m.mes, d.gestor_chave
