with folds as (
    select
        split_id,
        max(case when split_role = 'train' then label_window_end end)
            as max_train_label_window_end,
        min(case when split_role = 'validation' then prediction_timestamp end)
            as min_validation_prediction_timestamp,
        count_if(split_role = 'train') as train_row_count,
        count_if(split_role = 'validation') as validation_row_count
    from {{ ref('fct_training_assignments') }}
    where split_id in ('fold_1', 'fold_2', 'fold_3')
    group by split_id
)

select *
from folds
where
    train_row_count = 0
    or validation_row_count = 0
    or max_train_label_window_end > min_validation_prediction_timestamp
