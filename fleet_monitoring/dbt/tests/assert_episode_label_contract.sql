select *
from {{ ref('int_observacoes_elegiveis') }}
where
    not is_training_eligible
    or label_is_censored
    or (y_episode = 0 and next_episode_id is not null)
    or (y_episode = 0 and next_episode_start is not null)
    or (y_episode = 0 and lead_time_minutes is not null)
    or (y_episode = 1 and next_episode_id is null)
    or (y_episode = 1 and next_episode_start is null)
    or (y_episode = 1 and next_episode_start <= prediction_timestamp)
    or (
        y_episode = 1
        and next_episode_start > timestampadd(
            HOUR,
            {{ var('target_horizon_hours') }},
            prediction_timestamp
        )
    )
    or (y_episode = 1 and lead_time_minutes <= 0)
    or (
        y_episode = 1
        and lead_time_minutes > {{ var('target_horizon_hours') }} * 60
    )
