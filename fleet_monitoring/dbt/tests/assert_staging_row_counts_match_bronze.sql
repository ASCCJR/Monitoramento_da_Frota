with comparisons as (
    select
        'stg_apontamentos' as model_name,
        (select count(*) from {{ source('bronze_v2', 'apontamentos_raw') }}) as bronze_rows,
        (select count(*) from {{ ref('stg_apontamentos') }}) as staging_rows

    union all

    select
        'stg_telemetria' as model_name,
        (select count(*) from {{ source('bronze_v2', 'telemetria_raw') }}) as bronze_rows,
        (select count(*) from {{ ref('stg_telemetria') }}) as staging_rows

    union all

    select
        'stg_cma_regras' as model_name,
        (select count(*) from {{ source('bronze_v2', 'cma_regras_raw') }}) as bronze_rows,
        (select count(*) from {{ ref('stg_cma_regras') }}) as staging_rows

    union all

    select
        'stg_dicionario_dados' as model_name,
        (select count(*) from {{ source('bronze_v2', 'dicionario_dados_raw') }}) as bronze_rows,
        (select count(*) from {{ ref('stg_dicionario_dados') }}) as staging_rows
)

select *
from comparisons
where bronze_rows <> staging_rows
