-- Grão: fundo/classe (+ subclasse) por mês, para BI.
-- rentabilidade_mes = cota de fechamento / cota de fechamento do mês anterior - 1, calculada só
-- quando o mês anterior é o imediatamente anterior (lacuna de meses não gera retorno enganoso)
-- e as duas cotas são positivas. Cota <= 0 é defeito de dado: fica NULL e o teste a sinaliza.
with daily as (
    -- cast(... as date): no BigQuery o date_trunc do dbt devolve TIMESTAMP
    select *, cast({{ dbt.date_trunc('month', 'dt_comptc') }} as date) as mes
    from {{ ref('fct_fundo_dia') }}
),

flows as (
    select
        fundo_cnpj, id_subclasse, mes,
        count(*)                as dias_com_informe,
        sum(captacao)           as captacao_mes,
        sum(resgate)            as resgate_mes,
        sum(captacao_liquida)   as captacao_liquida_mes
    from daily
    group by fundo_cnpj, id_subclasse, mes
),

closing as (
    select
        fundo_cnpj, id_subclasse, mes,
        vl_patrim_liq           as pl_final,
        cotistas                as cotistas_final,
        case when vl_quota > 0 then vl_quota end as quota_final
    from (
        select
            *,
            row_number() over (partition by fundo_cnpj, id_subclasse, mes order by dt_comptc desc) as rn
        from daily
    ) as ranked
    where rn = 1
),

joined as (
    select
        f.fundo_cnpj, f.id_subclasse, f.mes,
        f.dias_com_informe, f.captacao_mes, f.resgate_mes, f.captacao_liquida_mes,
        c.pl_final, c.cotistas_final, c.quota_final,
        lag(c.quota_final) over (partition by f.fundo_cnpj, f.id_subclasse order by f.mes) as quota_mes_anterior,
        lag(f.mes)         over (partition by f.fundo_cnpj, f.id_subclasse order by f.mes) as mes_anterior
    from flows as f
    inner join closing as c
        on c.fundo_cnpj = f.fundo_cnpj and c.id_subclasse = f.id_subclasse and c.mes = f.mes
)

select
    {{ dbt.hash(dbt.concat(['fundo_cnpj', "'|'", 'id_subclasse', "'|'", 'cast(mes as ' ~ dbt.type_string() ~ ')'])) }} as agg_sk,
    fundo_cnpj, id_subclasse, mes,
    dias_com_informe, pl_final, cotistas_final, quota_final,
    captacao_mes, resgate_mes, captacao_liquida_mes,
    case
        when quota_mes_anterior > 0
         and quota_final > 0
         and {{ dbt.datediff('mes_anterior', 'mes', 'month') }} = 1
        -- double ANTES de dividir: um retorno é uma razão sem unidade, e a divisão de decimais de 12
        -- casas trunca de forma diferente em cada banco (BIGNUMERIC no BigQuery, DECIMAL no DuckDB)
        then cast(quota_final as {{ type_double() }}) / nullif(cast(quota_mes_anterior as {{ type_double() }}), 0) - 1
    end as rentabilidade_mes
from joined
