select
    evento_telemetria_id,
    event_time,
    availability_time,
    inicio_turno,
    fim_turno,
    dia_mes_origem
from {{ ref('stg_telemetria') }}
where
    availability_time <> event_time
    or event_time < inicio_turno
    or event_time > fim_turno
    or timestampdiff(MINUTE, inicio_turno, fim_turno) <> 720
    or dia_mes_origem <> day(event_time)
