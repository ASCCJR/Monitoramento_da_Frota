select *
from {{ ref('int_cma_evento_lookup') }}
where
    evento_key <> upper(trim(evento_key))
    or cma_rule_count <= 0
    or has_very_high_level not in (0, 1)
    or has_high_level not in (0, 1)
