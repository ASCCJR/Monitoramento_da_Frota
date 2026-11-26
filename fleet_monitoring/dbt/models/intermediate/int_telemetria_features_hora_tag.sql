with telemetry as (
    select
        evento_telemetria_id,
        event_time,
        availability_time,
        tag,
        alarme,
        criticidade,
        is_dont_go
    from {{ ref('stg_telemetria') }}
    where
        tag is not null
        and event_time is not null
        and availability_time is not null
),

cma_lookup as (
    select *
    from {{ ref('int_cma_evento_lookup') }}
),

enriched as (
    select
        telemetry.*,
        cma_lookup.cma_evento_id is not null as is_cma_associated,
        coalesce(cma_lookup.has_very_high_level, 0) as has_very_high_level,
        coalesce(cma_lookup.has_high_level, 0) as has_high_level
    from telemetry
    left join cma_lookup
        on upper(trim(telemetry.alarme)) = cma_lookup.evento_key
)

select
    sha2(
        to_json(named_struct(
            'contract_version', '{{ var("contract_version") }}',
            'tag', tag,
            'event_hour', date_trunc('HOUR', event_time)
        )),
        256
    ) as tag_hour_feature_id,
    '{{ var("contract_version") }}' as contract_version,
    tag,
    date_trunc('HOUR', event_time) as event_hour,
    count(*) as event_count,
    count_if(criticidade = 'Crítico') as critical_event_count,
    count_if(criticidade = 'Não Crítico') as noncritical_event_count,
    count_if(criticidade = 'Informacional') as informational_event_count,
    count_if(criticidade = 'Outros') as other_event_count,
    count_if(is_dont_go = 1) as dontgo_occurrence_count,
    count_if(is_cma_associated) as cma_event_count,
    count_if(is_cma_associated and has_very_high_level = 1)
        as cma_very_high_event_count,
    count_if(is_cma_associated and has_high_level = 1)
        as cma_high_event_count,
    min(event_time) as min_feature_event_time,
    max(event_time) as max_feature_event_time,
    max(availability_time) as max_feature_availability_time
from enriched
group by tag, date_trunc('HOUR', event_time)
