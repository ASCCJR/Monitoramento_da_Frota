with members as (
    select *
    from {{ ref('int_dontgo_episodio_membros') }}
),

aggregated as (
    select
        contract_version,
        episode_gap_hours,
        is_primary_gap,
        episode_id,
        tag,
        max(tipo_equipamento) as tipo_equipamento,
        max(tag_frota) as tag_frota,
        max(episode_sequence) as episode_sequence,
        min(episode_anchor_time) as episode_start_proxy,
        max(event_time) as episode_end,
        count(*) as occurrence_count,
        max(case when is_episode_anchor then ocorrencia_id end) as anchor_occurrence_id,
        max(cast(episode_is_left_censored as int)) = 1 as episode_is_left_censored,
        min(availability_time) as first_occurrence_availability_time,
        min(dataset_start_time) as dataset_start_time,
        max(dataset_end_time) as dataset_end_time
    from members
    group by
        contract_version,
        episode_gap_hours,
        is_primary_gap,
        episode_id,
        tag
),

with_unavailability_censoring as (
    select
        aggregated.*,
        cast(count(unavailability.unavailability_id) > 0 as boolean)
            as episode_start_history_is_unavailable
    from aggregated
    left join {{ ref('int_telemetria_indisponibilidade_global') }} as unavailability
        on unavailability.unavailable_start_exclusive < aggregated.episode_start_proxy
        and unavailability.unavailable_end_exclusive > timestampadd(
            HOUR,
            -aggregated.episode_gap_hours,
            aggregated.episode_start_proxy
        )
    group by all
)

select
    contract_version,
    episode_gap_hours,
    is_primary_gap,
    episode_id,
    episode_sequence,
    tag,
    tipo_equipamento,
    tag_frota,
    episode_start_proxy,
    episode_end,
    timestampdiff(SECOND, episode_start_proxy, episode_end) as episode_duration_seconds,
    occurrence_count,
    anchor_occurrence_id,
    episode_is_left_censored,
    episode_start_history_is_unavailable,
    cast(
        not episode_is_left_censored
        and not episode_start_history_is_unavailable
        as boolean
    ) as episode_start_is_observed,
    first_occurrence_availability_time,
    dataset_start_time,
    dataset_end_time
from with_unavailability_censoring
