with expected as (
    select count(*) as row_count
    from {{ ref('int_observacoes_elegiveis') }}
),

actual as (
    select
        count(*) as row_count,
        count_if(not is_training_eligible) as ineligible_count,
        count_if(label_is_censored) as censored_count,
        count_if(y_episode = 1 and next_episode_id is null) as missing_episode_count,
        count_if(
            y_episode = 1
            and (lead_time_minutes <= 0 or lead_time_minutes > 240)
        ) as invalid_lead_time_count
    from {{ ref('fct_training_set') }}
)

select *
from expected
cross join actual
where
    expected.row_count <> actual.row_count
    or actual.ineligible_count <> 0
    or actual.censored_count <> 0
    or actual.missing_episode_count <> 0
    or actual.invalid_lead_time_count <> 0
