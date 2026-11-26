with telemetry as (
    select *
    from {{ ref('stg_telemetria') }}
)

select
    sha2(
        to_json(named_struct(
            'contract_version', '{{ var("contract_version") }}',
            'tag', tag,
            'event_hour', date_trunc('HOUR', event_time)
        )),
        256
    ) as tag_hour_id,
    '{{ var("contract_version") }}' as contract_version,
    tag,
    max(tipo_equipamento) as tipo_equipamento,
    date_trunc('HOUR', event_time) as event_hour,
    min(event_time) as first_event_time,
    max(event_time) as last_event_time,
    count(*) as event_count,
    count_if(is_dont_go = 1) as dont_go_count,
    count_if(criticidade_id = 1) as critical_event_count,
    count_if(criticidade_id = 2) as noncritical_event_count,
    count_if(criticidade_id = 3) as informational_event_count,
    count_if(criticidade_id = 4) as other_criticality_event_count,
    count(distinct alarme_id) as distinct_alarm_count
from telemetry
group by tag, date_trunc('HOUR', event_time)
