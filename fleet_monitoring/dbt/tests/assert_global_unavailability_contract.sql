with ordered as (
    select
        *,
        lag(unavailable_end_exclusive) over (
            order by unavailable_start_exclusive
        ) as previous_unavailable_end
    from {{ ref('int_telemetria_indisponibilidade_global') }}
)

select *
from ordered
where
    unavailable_start_exclusive >= unavailable_end_exclusive
    or gap_duration_seconds < threshold_seconds
    or zero_full_5m_bin_count < 1
    or unavailable_start_exclusive < previous_unavailable_end
