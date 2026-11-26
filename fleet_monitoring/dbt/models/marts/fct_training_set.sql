with features as (
    select *
    from {{ ref('fct_features_scoring') }}
),

labels as (
    select
        scoring_row_id,
        y_episode,
        next_episode_id,
        next_episode_start,
        lead_time_minutes,
        label_is_censored
    from {{ ref('int_observacoes_elegiveis') }}
)

select
    features.*,
    labels.y_episode,
    labels.next_episode_id,
    labels.next_episode_start,
    labels.lead_time_minutes,
    labels.label_is_censored
from features
inner join labels
    on features.scoring_row_id = labels.scoring_row_id
