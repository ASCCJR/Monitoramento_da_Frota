with ordered as (
    select
        *,
        lag(prediction_timestamp) over (
            order by prediction_timestamp
        ) as previous_prediction_timestamp
    from {{ ref('int_scoring_cutoffs') }}
)

select *
from ordered
where
    typeof(prediction_timestamp) <> 'timestamp_ntz'
    or minute(prediction_timestamp) <> 0
    or second(prediction_timestamp) <> 0
    or not has_complete_global_lookback
    or not has_complete_global_followup
    or (
        previous_prediction_timestamp is not null
        and timestampdiff(
            HOUR,
            previous_prediction_timestamp,
            prediction_timestamp
        ) <> {{ var('scoring_frequency_hours') }}
    )
