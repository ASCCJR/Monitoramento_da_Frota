with telemetry_tags as (
    select distinct
        tag,
        tipo_equipamento
    from {{ ref('stg_telemetria') }}
),

apontamento_tags as (
    select distinct
        tag,
        tipo_equipamento
    from {{ ref('stg_apontamentos') }}
)

select telemetry_tags.*
from telemetry_tags
left join apontamento_tags
    on telemetry_tags.tag = apontamento_tags.tag
    and telemetry_tags.tipo_equipamento = apontamento_tags.tipo_equipamento
where apontamento_tags.tag is null
