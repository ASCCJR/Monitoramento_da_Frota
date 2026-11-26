with cutoffs as (
    select *
    from {{ ref('int_scoring_cutoffs') }}
),

equipment as (
    select *
    from {{ ref('int_equipamentos') }}
),

hourly_telemetry as (
    select *
    from {{ ref('int_telemetria_hora_tag') }}
),

occurrences as (
    select *
    from {{ ref('int_dontgo_ocorrencias') }}
),

unavailability as (
    select *
    from {{ ref('int_telemetria_indisponibilidade_global') }}
),

grid as (
    select
        cutoffs.cutoff_id,
        cutoffs.prediction_timestamp,
        cutoffs.dataset_start_time,
        cutoffs.dataset_end_time,
        cutoffs.has_complete_global_lookback,
        cutoffs.has_complete_global_followup,
        equipment.equipamento_id,
        equipment.tag,
        equipment.tipo_equipamento,
        equipment.tag_frota,
        equipment.localidade,
        equipment.tag_first_event_time,
        equipment.tag_last_event_time
    from cutoffs
    cross join equipment
),

activity as (
    select
        grid.cutoff_id,
        grid.tag,
        sum(hourly_telemetry.event_count) as active_event_count,
        min(hourly_telemetry.first_event_time) as active_first_event_time,
        max(hourly_telemetry.last_event_time) as active_last_event_time
    from grid
    inner join hourly_telemetry
        on grid.tag = hourly_telemetry.tag
        and hourly_telemetry.event_hour >= timestampadd(
            HOUR,
            -{{ var('active_lookback_hours') }},
            grid.prediction_timestamp
        )
        and hourly_telemetry.event_hour < grid.prediction_timestamp
    group by grid.cutoff_id, grid.tag
),

washout as (
    select
        grid.cutoff_id,
        grid.tag,
        count(occurrences.ocorrencia_id) as washout_occurrence_count,
        max(occurrences.event_time) as latest_washout_occurrence_time
    from grid
    inner join occurrences
        on grid.tag = occurrences.tag
        and occurrences.event_time >= timestampadd(
            HOUR,
            -{{ var('washout_hours') }},
            grid.prediction_timestamp
        )
        and occurrences.event_time <= grid.prediction_timestamp
    group by grid.cutoff_id, grid.tag
),

cutoff_unavailability as (
    select
        cutoffs.cutoff_id,
        max(cast(
            unavailability.unavailable_start_exclusive < cutoffs.prediction_timestamp
            and unavailability.unavailable_end_exclusive > timestampadd(
                HOUR,
                -{{ var('active_lookback_hours') }},
                cutoffs.prediction_timestamp
            )
            as int
        )) = 1 as has_lookback_unavailability,
        max(cast(
            unavailability.unavailable_start_exclusive < timestampadd(
                HOUR,
                {{ var('target_horizon_hours') }},
                cutoffs.prediction_timestamp
            )
            and unavailability.unavailable_end_exclusive > cutoffs.prediction_timestamp
            as int
        )) = 1 as has_followup_unavailability
    from cutoffs
    left join unavailability
        on unavailability.unavailable_start_exclusive < timestampadd(
            HOUR,
            {{ var('target_horizon_hours') }},
            cutoffs.prediction_timestamp
        )
        and unavailability.unavailable_end_exclusive > timestampadd(
            HOUR,
            -{{ var('active_lookback_hours') }},
            cutoffs.prediction_timestamp
        )
    group by cutoffs.cutoff_id
),

with_flags as (
    select
        grid.*,
        coalesce(activity.active_event_count, 0) as active_event_count,
        activity.active_first_event_time,
        activity.active_last_event_time,
        coalesce(washout.washout_occurrence_count, 0) as washout_occurrence_count,
        washout.latest_washout_occurrence_time,
        coalesce(cutoff_unavailability.has_lookback_unavailability, false)
            as has_lookback_unavailability,
        coalesce(cutoff_unavailability.has_followup_unavailability, false)
            as has_followup_unavailability,
        cast(
            timestampadd(
                HOUR,
                -{{ var('active_lookback_hours') }},
                grid.prediction_timestamp
            ) < grid.tag_first_event_time
            as boolean
        ) as tag_is_cold_start
    from grid
    left join activity
        on grid.cutoff_id = activity.cutoff_id
        and grid.tag = activity.tag
    left join washout
        on grid.cutoff_id = washout.cutoff_id
        and grid.tag = washout.tag
    left join cutoff_unavailability
        on grid.cutoff_id = cutoff_unavailability.cutoff_id
),

with_scoring_eligibility as (
    select
        *,
        cast(
            has_complete_global_lookback
            and active_event_count > 0
            and washout_occurrence_count = 0
            and not has_lookback_unavailability
            as boolean
        ) as is_scoring_eligible
    from with_flags
)

select
    sha2(
        to_json(named_struct(
            'contract_version', '{{ var("contract_version") }}',
            'tag', tag,
            'prediction_timestamp', prediction_timestamp
        )),
        256
    ) as scoring_row_id,
    '{{ var("contract_version") }}' as contract_version,
    cutoff_id,
    prediction_timestamp,
    equipamento_id,
    tag,
    tipo_equipamento,
    tag_frota,
    localidade,
    tag_first_event_time,
    tag_last_event_time,
    tag_is_cold_start,
    active_event_count,
    active_first_event_time,
    active_last_event_time,
    washout_occurrence_count,
    latest_washout_occurrence_time,
    has_complete_global_lookback,
    has_complete_global_followup,
    has_lookback_unavailability,
    has_followup_unavailability,
    is_scoring_eligible,
    cast(
        is_scoring_eligible
        and has_complete_global_followup
        and not has_followup_unavailability
        as boolean
    ) as is_training_eligible,
    dataset_start_time,
    dataset_end_time
from with_scoring_eligibility
