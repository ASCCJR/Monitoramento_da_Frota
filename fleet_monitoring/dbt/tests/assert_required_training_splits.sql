with expected as (
    select * from values
        ('fold_1', 'train'),
        ('fold_1', 'validation'),
        ('fold_2', 'train'),
        ('fold_2', 'validation'),
        ('fold_3', 'train'),
        ('fold_3', 'validation'),
        ('final_train_jan_apr', 'train'),
        ('calibration_may_01_15', 'calibration'),
        ('policy_may_16_31', 'policy'),
        ('holdout_june', 'holdout')
        as expected(split_id, split_role)
),

actual as (
    select distinct split_id, split_role
    from {{ ref('fct_training_assignments') }}
)

select expected.*
from expected
left join actual
    on expected.split_id = actual.split_id
    and expected.split_role = actual.split_role
where actual.split_id is null
