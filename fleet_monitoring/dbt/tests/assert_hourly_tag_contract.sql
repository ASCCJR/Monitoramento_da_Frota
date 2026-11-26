select *
from {{ ref('int_telemetria_hora_tag') }}
where
    event_count <= 0
    or dont_go_count < 0
    or dont_go_count > event_count
    or first_event_time < event_hour
    or first_event_time >= timestampadd(HOUR, 1, event_hour)
    or last_event_time < event_hour
    or last_event_time >= timestampadd(HOUR, 1, event_hour)
    or first_event_time > last_event_time
