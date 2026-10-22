-- Fluxo e patrimônio por mês e classe de fundo. Fundos presentes no informe e ausentes do
-- cadastro aparecem como 'SEM CADASTRO' (em vez de sumirem em silêncio de um INNER JOIN).
select
    m.mes,
    coalesce(d.classe, 'SEM CADASTRO')              as classe,
    count(distinct m.fundo_cnpj)                    as fundos,
    sum(m.pl_final)                                 as pl_total,
    sum(m.captacao_mes)                             as captacao,
    sum(m.resgate_mes)                              as resgate,
    sum(m.captacao_liquida_mes)                     as captacao_liquida
from {{ ref('agg_fundo_mensal') }} as m
left join {{ ref('dim_fundos') }} as d on d.fundo_cnpj = m.fundo_cnpj
group by m.mes, coalesce(d.classe, 'SEM CADASTRO')
