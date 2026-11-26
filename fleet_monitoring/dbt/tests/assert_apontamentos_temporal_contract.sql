select
    apontamento_id,
    inicio,
    fim,
    availability_time,
    duracao_segundos
from {{ ref('stg_apontamentos') }}
where
    event_time <> inicio
    or availability_time <> fim
    or fim <= inicio
    or duracao_segundos <= 0
    or duracao_segundos <> timestampdiff(SECOND, inicio, fim)
