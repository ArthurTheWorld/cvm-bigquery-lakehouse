-- Uma linha por gestor (PJ pelo CNPJ; PF pelo hash do CPF).
select
    gestor_chave,
    max(gestor_tipo)                                as gestor_tipo,
    max(gestor_nome)                                as gestor_nome,
    count(*)                                        as fundos_cadastrados,
    sum(case when is_ativo then 1 else 0 end)       as fundos_ativos
from {{ ref('dim_fundos') }}
group by gestor_chave
