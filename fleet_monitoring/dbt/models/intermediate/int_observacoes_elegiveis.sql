with eligible_grid as (
    select *
    from {{ ref('int_scoring_grade') }}
    where is_training_eligible
),

primary_observed_episodes as (
    select *
    from {{ ref('int_dontgo_episodios') }}
    where
        is_primary_gap
        and episode_start_is_observed
),

next_episode_times as (
    select
        eligible_grid.scoring_row_id,
        min(primary_observed_episodes.episode_start_proxy) as next_episode_start
    from eligible_grid
    left join primary_observed_episodes
        on eligible_grid.tag = primary_observed_episodes.tag
        and primary_observed_episodes.episode_start_proxy
            > eligible_grid.prediction_timestamp
        and primary_observed_episodes.episode_start_proxy <= timestampadd(
            HOUR,
            {{ var('target_horizon_hours') }},
            eligible_grid.prediction_timestamp
        )
    group by eligible_grid.scoring_row_id
),

matched_episodes as (
    select
        next_episode_times.scoring_row_id,
        next_episode_times.next_episode_start,
        primary_observed_episodes.episode_id as next_episode_id
    from next_episode_times
    left join eligible_grid
        on next_episode_times.scoring_row_id = eligible_grid.scoring_row_id
    left join primary_observed_episodes
        on eligible_grid.tag = primary_observed_episodes.tag
        and next_episode_times.next_episode_start
            = primary_observed_episodes.episode_start_proxy
)

select
    eligible_grid.*,
    cast(matched_episodes.next_episode_start is not null as tinyint) as y_episode,
    matched_episodes.next_episode_id,
    matched_episodes.next_episode_start,
    case
        when matched_episodes.next_episode_start is not null
            then cast(
                timestampdiff(
                    MICROSECOND,
                    eligible_grid.prediction_timestamp,
                    matched_episodes.next_episode_start
                ) / 60000000.0
                as double
            )
    end as lead_time_minutes,
    cast(false as boolean) as label_is_censored
from eligible_grid
left join matched_episodes
    on eligible_grid.scoring_row_id = matched_episodes.scoring_row_id
