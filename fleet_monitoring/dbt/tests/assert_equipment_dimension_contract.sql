select *
from {{ ref('int_equipamentos') }}
where
    tipo_count <> 1
    or frota_count <> 1
    or localidade_count <> 1
    or tag_first_event_time > tag_last_event_time
    or telemetry_event_count <= 0
