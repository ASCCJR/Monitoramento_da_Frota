with telemetry as (
    select *
    from {{ ref('stg_telemetria') }}
),

dataset_bounds as (
    select
        min(event_time) as dataset_start_time,
        max(event_time) as dataset_end_time
    from telemetry
),

occurrences as (
    select
        telemetry.evento_telemetria_id as ocorrencia_id,
        telemetry.event_time,
        telemetry.availability_time,
        telemetry.availability_is_proxy,
        telemetry.tag,
        telemetry.tag_frota,
        telemetry.tipo_equipamento,
        telemetry.alarme_id,
        telemetry.alarme,
        telemetry.criticidade_id,
        telemetry.criticidade,
        telemetry.valor,
        telemetry.classe,
        telemetry.row_hash,
        telemetry.source_path,
        telemetry.source_sha256,
        telemetry.source_period,
        telemetry.manifest_version,
        telemetry.ingestion_time,
        dataset_bounds.dataset_start_time,
        dataset_bounds.dataset_end_time
    from telemetry
    cross join dataset_bounds
    where telemetry.is_dont_go = 1
),

sequenced as (
    select
        *,
        row_number() over (
            partition by tag
            order by event_time, ocorrencia_id, row_hash
        ) as ocorrencia_sequence
    from occurrences
)

select
    ocorrencia_id,
    event_time,
    availability_time,
    availability_is_proxy,
    tag,
    tag_frota,
    tipo_equipamento,
    alarme_id,
    alarme,
    criticidade_id,
    criticidade,
    valor,
    classe,
    row_hash,
    ocorrencia_sequence,
    dataset_start_time,
    dataset_end_time,
    source_path,
    source_sha256,
    source_period,
    manifest_version,
    ingestion_time
from sequenced
