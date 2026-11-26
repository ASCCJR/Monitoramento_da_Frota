with source_data as (
    select *
    from {{ source('bronze_v2', 'cma_regras_raw') }}
),

normalized as (
    select
        upper(trim(TIPO)) as tipo_regra,
        trim(EVENTO) as evento,
        trim(SITUACAO) as situacao,
        cast(QUANTIDADE as int) as quantidade,
        cast(TEMPO as int) as janela_minutos,
        initcap(lower(trim(NIVEL))) as nivel,
        cast(TEMPO = 0 as boolean) as regra_imediata,
        _source_path as source_path,
        _source_sha256 as source_sha256,
        _source_period as source_period,
        _manifest_version as manifest_version,
        cast(_ingested_at_utc as timestamp) as ingestion_time
    from source_data
),

identified as (
    select
        sha2(
            to_json(named_struct(
                'tipo_regra', tipo_regra,
                'evento', evento,
                'situacao', situacao,
                'quantidade', quantidade,
                'janela_minutos', janela_minutos,
                'nivel', nivel
            )),
            256
        ) as cma_regra_id,
        *
    from normalized
)

select
    cma_regra_id,
    tipo_regra,
    evento,
    situacao,
    quantidade,
    janela_minutos,
    nivel,
    regra_imediata,
    source_path,
    source_sha256,
    source_period,
    manifest_version,
    ingestion_time
from identified
