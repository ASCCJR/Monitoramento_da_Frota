with telemetry as (
    select *
    from {{ ref('stg_telemetria') }}
),

equipment_dimension as (
    select
        tag,
        max(tipo_equipamento) as tipo_equipamento,
        max(tag_frota) as tag_frota,
        max(localidade) as localidade,
        count(distinct tipo_equipamento) as tipo_count,
        count(distinct tag_frota) as frota_count,
        count(distinct localidade) as localidade_count,
        min(event_time) as tag_first_event_time,
        max(event_time) as tag_last_event_time,
        count(*) as telemetry_event_count
    from telemetry
    group by tag
)

select
    sha2(
        to_json(named_struct(
            'contract_version', '{{ var("contract_version") }}',
            'tag', tag
        )),
        256
    ) as equipamento_id,
    '{{ var("contract_version") }}' as contract_version,
    tag,
    tipo_equipamento,
    tag_frota,
    localidade,
    tipo_count,
    frota_count,
    localidade_count,
    tag_first_event_time,
    tag_last_event_time,
    telemetry_event_count
from equipment_dimension
