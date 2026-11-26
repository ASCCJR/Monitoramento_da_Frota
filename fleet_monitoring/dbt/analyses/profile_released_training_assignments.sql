select
    assignments.split_id,
    assignments.split_role,
    count(*) as row_count,
    sum(training.y_episode) as positive_count,
    cast(avg(training.y_episode) as decimal(18, 8)) as positive_prevalence,
    min(assignments.prediction_timestamp) as first_prediction_timestamp,
    max(assignments.prediction_timestamp) as last_prediction_timestamp,
    max(assignments.label_window_end) as max_label_window_end
from {{ ref('fct_training_assignments') }} as assignments
inner join {{ ref('fct_training_set') }} as training
    on assignments.scoring_row_id = training.scoring_row_id
where assignments.is_released
group by assignments.split_id, assignments.split_role
order by assignments.split_id, assignments.split_role
