with training_rows as (
    select
        scoring_row_id,
        prediction_timestamp,
        timestampadd(
            HOUR,
            {{ var('target_horizon_hours') }},
            prediction_timestamp
        ) as label_window_end
    from {{ ref('fct_training_set') }}
),

assignments as (
    select
        'fold_1' as split_id,
        'train' as split_role,
        timestamp_ntz'2025-01-01 00:00:00' as period_start,
        timestamp_ntz'2025-02-01 00:00:00' as period_end_exclusive,
        *
    from training_rows
    where
        prediction_timestamp >= timestamp_ntz'2025-01-01 00:00:00'
        and prediction_timestamp < timestamp_ntz'2025-02-01 00:00:00'
        and label_window_end <= timestamp_ntz'2025-02-01 00:00:00'

    union all

    select
        'fold_1',
        'validation',
        timestamp_ntz'2025-02-01 00:00:00',
        timestamp_ntz'2025-03-01 00:00:00',
        *
    from training_rows
    where
        prediction_timestamp >= timestamp_ntz'2025-02-01 00:00:00'
        and prediction_timestamp < timestamp_ntz'2025-03-01 00:00:00'
        and label_window_end <= timestamp_ntz'2025-03-01 00:00:00'

    union all

    select
        'fold_2',
        'train',
        timestamp_ntz'2025-01-01 00:00:00',
        timestamp_ntz'2025-03-01 00:00:00',
        *
    from training_rows
    where
        prediction_timestamp >= timestamp_ntz'2025-01-01 00:00:00'
        and prediction_timestamp < timestamp_ntz'2025-03-01 00:00:00'
        and label_window_end <= timestamp_ntz'2025-03-01 00:00:00'

    union all

    select
        'fold_2',
        'validation',
        timestamp_ntz'2025-03-01 00:00:00',
        timestamp_ntz'2025-04-01 00:00:00',
        *
    from training_rows
    where
        prediction_timestamp >= timestamp_ntz'2025-03-01 00:00:00'
        and prediction_timestamp < timestamp_ntz'2025-04-01 00:00:00'
        and label_window_end <= timestamp_ntz'2025-04-01 00:00:00'

    union all

    select
        'fold_3',
        'train',
        timestamp_ntz'2025-01-01 00:00:00',
        timestamp_ntz'2025-04-01 00:00:00',
        *
    from training_rows
    where
        prediction_timestamp >= timestamp_ntz'2025-01-01 00:00:00'
        and prediction_timestamp < timestamp_ntz'2025-04-01 00:00:00'
        and label_window_end <= timestamp_ntz'2025-04-01 00:00:00'

    union all

    select
        'fold_3',
        'validation',
        timestamp_ntz'2025-04-01 00:00:00',
        timestamp_ntz'2025-05-01 00:00:00',
        *
    from training_rows
    where
        prediction_timestamp >= timestamp_ntz'2025-04-01 00:00:00'
        and prediction_timestamp < timestamp_ntz'2025-05-01 00:00:00'
        and label_window_end <= timestamp_ntz'2025-05-01 00:00:00'

    union all

    select
        'final_train_jan_apr',
        'train',
        timestamp_ntz'2025-01-01 00:00:00',
        timestamp_ntz'2025-05-01 00:00:00',
        *
    from training_rows
    where
        prediction_timestamp >= timestamp_ntz'2025-01-01 00:00:00'
        and prediction_timestamp < timestamp_ntz'2025-05-01 00:00:00'
        and label_window_end <= timestamp_ntz'2025-05-01 00:00:00'

    union all

    select
        'calibration_may_01_15',
        'calibration',
        timestamp_ntz'2025-05-01 00:00:00',
        timestamp_ntz'2025-05-16 00:00:00',
        *
    from training_rows
    where
        prediction_timestamp >= timestamp_ntz'2025-05-01 00:00:00'
        and prediction_timestamp < timestamp_ntz'2025-05-16 00:00:00'
        and label_window_end <= timestamp_ntz'2025-05-16 00:00:00'

    union all

    select
        'policy_may_16_31',
        'policy',
        timestamp_ntz'2025-05-16 00:00:00',
        timestamp_ntz'2025-06-01 00:00:00',
        *
    from training_rows
    where
        prediction_timestamp >= timestamp_ntz'2025-05-16 00:00:00'
        and prediction_timestamp < timestamp_ntz'2025-06-01 00:00:00'
        and label_window_end <= timestamp_ntz'2025-06-01 00:00:00'

    union all

    select
        'holdout_june',
        'holdout',
        timestamp_ntz'2025-06-01 00:00:00',
        timestamp_ntz'2025-07-01 00:00:00',
        *
    from training_rows
    where
        prediction_timestamp >= timestamp_ntz'2025-06-01 00:00:00'
        and prediction_timestamp < timestamp_ntz'2025-07-01 00:00:00'
        and label_window_end <= timestamp_ntz'2025-07-01 00:00:00'
)

select
    sha2(
        to_json(named_struct(
            'split_version', '1.0.0',
            'split_id', split_id,
            'split_role', split_role,
            'scoring_row_id', scoring_row_id
        )),
        256
    ) as assignment_id,
    '1.0.0' as split_version,
    split_id,
    split_role,
    scoring_row_id,
    prediction_timestamp,
    label_window_end,
    period_start,
    period_end_exclusive,
    cast(split_role = 'holdout' as boolean) as is_holdout,
    cast(split_role <> 'holdout' as boolean) as is_released
from assignments
