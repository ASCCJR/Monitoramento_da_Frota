with episodes as (
    select
        episode_id,
        episode_start_proxy,
        tag,
        tipo_equipamento
    from {{ ref('int_dontgo_episodios') }}
    where
        is_primary_gap
        and episode_start_is_observed
),

candidates as (
    select
        episodes.episode_id,
        episodes.episode_start_proxy,
        episodes.tipo_equipamento,
        count(grid.scoring_row_id) as candidate_cutoff_count,
        count_if(grid.active_event_count > 0) as active_cutoff_count,
        count_if(
            grid.active_event_count > 0
            and grid.washout_occurrence_count = 0
        ) as active_washout_free_cutoff_count,
        count_if(grid.is_scoring_eligible) as scoring_eligible_cutoff_count,
        count_if(grid.is_training_eligible) as training_eligible_cutoff_count
    from episodes
    left join {{ ref('int_scoring_grade') }} as grid
        on episodes.episode_start_proxy > grid.prediction_timestamp
        and episodes.episode_start_proxy <= timestampadd(
            HOUR,
            {{ var('target_horizon_hours') }},
            grid.prediction_timestamp
        )
        and grid.tag = episodes.tag
    group by
        episodes.episode_id,
        episodes.episode_start_proxy,
        episodes.tipo_equipamento
),

classified as (
    select
        *,
        case
            when candidate_cutoff_count = 0 then 'sem cutoff horario candidato'
            when active_cutoff_count = 0 then 'sem atividade no lookback'
            when active_washout_free_cutoff_count = 0 then 'todos os cutoffs em washout'
            when scoring_eligible_cutoff_count = 0 then 'indisponibilidade no lookback'
            when training_eligible_cutoff_count = 0 then 'follow-up indisponivel'
            else 'targetable'
        end as targetability_reason
    from candidates
)

select
    targetability_reason,
    count(*) as episode_count
from classified
group by targetability_reason
order by episode_count desc, targetability_reason
