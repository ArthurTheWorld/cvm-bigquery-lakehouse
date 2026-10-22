-- Uma linha por fundo (último snapshot do cadastro).
-- LGPD: gestor PJ mostra o nome; gestor PF (pessoa) aparece pseudonimizado, a partir do
-- hash do CPF. O CPF e o diretor responsável não existem nesta camada.
select
    fundo_cnpj,
    denom_social                                    as nome,
    tp_fundo,
    situacao,
    situacao = 'EM FUNCIONAMENTO NORMAL'            as is_ativo,
    classe,
    classe_anbima,
    publico_alvo,
    condominio,
    fundo_de_cotas,
    fundo_exclusivo,
    dt_registro,
    dt_inicio_atividade,
    dt_cancelamento,
    taxa_adm,
    taxa_perfm,
    administrador_cnpj,
    administrador,
    gestor_tipo,
    gestor_chave,
    case
        when gestor_tipo = 'PJ' then gestor_nome_pj
        else 'PESSOA FÍSICA ' || substr(gestor_chave, 1, 8)
    end                                             as gestor_nome
from {{ ref('stg_cad_fi') }}
