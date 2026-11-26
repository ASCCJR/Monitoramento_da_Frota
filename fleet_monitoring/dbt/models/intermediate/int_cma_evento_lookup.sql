with normalized as (
    select
        upper(trim(evento)) as evento_key,
        evento,
        nivel,
        cma_regra_id
    from {{ ref('stg_cma_regras') }}
    where nullif(trim(evento), '') is not null
)

select
    sha2(
        to_json(named_struct(
            'contract_version', '{{ var("contract_version") }}',
            'evento_key', evento_key
        )),
        256
    ) as cma_evento_id,
    '{{ var("contract_version") }}' as contract_version,
    evento_key,
    min(evento) as evento_exemplo,
    count(*) as cma_rule_count,
    cast(max(cast(nivel = 'Muito Alto' as int)) as tinyint)
        as has_very_high_level,
    cast(max(cast(nivel = 'Alto' as int)) as tinyint)
        as has_high_level
from normalized
group by evento_key
