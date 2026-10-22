{#- Último snapshot do cadastro, uma linha por fundo.

    LGPD / minimização de dados: o CPF de gestor pessoa física e o nome do diretor
    responsável existem no raw, mas AQUI o CPF vira um hash estável (gestor_chave) e o
    diretor é descartado. Nada abaixo do raw carrega CPF. -#}
{%- set src = source('raw', 'cad_fi') -%}

with latest as (
    select * from {{ src }}
    where _src_snapshot = (select max(_src_snapshot) from {{ src }})
),

ranked as (
    select
        *,
        row_number() over (
            partition by {{ digits_only('cnpj_fundo') }}
            order by _loaded_at desc, _src_row desc
        ) as rn
    from latest
),

typed as (
    select
        {{ digits_only('cnpj_fundo') }}                                     as fundo_cnpj,
        {{ col_or_null(src, 'tp_fundo') }}                                  as tp_fundo,
        denom_social,
        sit                                                                 as situacao,
        {{ col_or_null(src, 'classe') }}                                    as classe,
        {{ col_or_null(src, 'classe_anbima') }}                             as classe_anbima,
        {{ col_or_null(src, 'publico_alvo') }}                              as publico_alvo,
        {{ col_or_null(src, 'condom') }}                                    as condominio,
        {{ col_or_null(src, 'fundo_cotas') }} = 'S'                         as fundo_de_cotas,
        {{ col_or_null(src, 'fundo_exclusivo') }} = 'S'                     as fundo_exclusivo,
        {{ dbt.safe_cast(col_or_null(src, 'dt_reg'), 'date') }}             as dt_registro,
        {{ dbt.safe_cast(col_or_null(src, 'dt_ini_ativ'), 'date') }}        as dt_inicio_atividade,
        {{ dbt.safe_cast(col_or_null(src, 'dt_cancel'), 'date') }}          as dt_cancelamento,
        {{ dbt.safe_cast(col_or_null(src, 'taxa_adm'), dbt.type_numeric()) }}   as taxa_adm,
        {{ dbt.safe_cast(col_or_null(src, 'taxa_perfm'), dbt.type_numeric()) }} as taxa_perfm,
        {{ digits_only(col_or_null(src, 'cnpj_admin')) }}                   as administrador_cnpj,
        {{ col_or_null(src, 'admin') }}                                     as administrador,
        {{ col_or_null(src, 'pf_pj_gestor') }}                              as gestor_tipo,
        {{ col_or_null(src, 'gestor') }}                                    as gestor_nome_original,
        {{ digits_only(col_or_null(src, 'cpf_cnpj_gestor')) }}              as gestor_documento,
        _src_snapshot                                                       as snapshot_date
    from ranked
    where rn = 1
)

select
    fundo_cnpj,
    tp_fundo,
    denom_social,
    situacao,
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
    -- Chave estável do gestor: o CNPJ (público) para PJ; para PF, o HASH do CPF.
    case when gestor_tipo = 'PJ' then gestor_documento else {{ sha256_hex('gestor_documento') }} end as gestor_chave,
    -- Nome: PJ mantém; PF (pessoa) é pseudonimizado.
    case when gestor_tipo = 'PJ' then gestor_nome_original else null end as gestor_nome_pj,
    snapshot_date
from typed
