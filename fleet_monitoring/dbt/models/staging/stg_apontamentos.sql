with source_data as (
    select *
    from {{ source('bronze_v2', 'apontamentos_raw') }}
),

renamed as (
    select
        cast(Id as bigint) as apontamento_id,
        cast(Inicio as timestamp_ntz) as event_time,
        cast(Inicio as timestamp_ntz) as inicio,
        cast(Fim as timestamp_ntz) as fim,
        cast(Fim as timestamp_ntz) as availability_time,
        cast(true as boolean) as availability_is_proxy,
        timestampdiff(SECOND, Inicio, Fim) as duracao_segundos,
        trim(Tag) as tag,
        trim(Frota) as frota,
        trim(Tipo) as tipo_equipamento,
        trim(Classe) as classe_operacional,
        sha2(
            to_json(named_struct(
                'apontamento_id', Id,
                'inicio', Inicio,
                'fim', Fim,
                'tag', Tag,
                'frota', Frota,
                'tipo', Tipo,
                'classe', Classe
            )),
            256
        ) as row_hash,
        _source_path as source_path,
        _source_sha256 as source_sha256,
        _source_period as source_period,
        _manifest_version as manifest_version,
        cast(_ingested_at_utc as timestamp) as ingestion_time
    from source_data
)

select
    apontamento_id,
    event_time,
    inicio,
    fim,
    availability_time,
    availability_is_proxy,
    duracao_segundos,
    tag,
    frota,
    tipo_equipamento,
    classe_operacional,
    row_hash,
    source_path,
    source_sha256,
    source_period,
    manifest_version,
    ingestion_time
from renamed
