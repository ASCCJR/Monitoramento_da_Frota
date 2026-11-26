with dataset_bounds as (
    select
        min(event_time) as dataset_start_time,
        max(event_time) as dataset_end_time
    from {{ ref('stg_telemetria') }}
),

aligned_bounds as (
    select
        dataset_start_time,
        dataset_end_time,
        case
            when dataset_start_time = date_trunc('HOUR', dataset_start_time)
                then timestampadd(
                    HOUR,
                    {{ var('active_lookback_hours') }},
                    dataset_start_time
                )
            else timestampadd(
                HOUR,
                {{ var('active_lookback_hours') + 1 }},
                date_trunc('HOUR', dataset_start_time)
            )
        end as first_prediction_timestamp,
        date_trunc(
            'HOUR',
            timestampadd(
                HOUR,
                -{{ var('target_horizon_hours') }},
                dataset_end_time
            )
        ) as last_prediction_timestamp
    from dataset_bounds
),

raw_cutoffs as (
    select
        dataset_start_time,
        dataset_end_time,
        explode(sequence(
            first_prediction_timestamp,
            last_prediction_timestamp,
            interval {{ var('scoring_frequency_hours') }} hour
        )) as prediction_timestamp_raw
    from aligned_bounds
),

cutoffs as (
    select
        dataset_start_time,
        dataset_end_time,
        cast(prediction_timestamp_raw as timestamp_ntz) as prediction_timestamp
    from raw_cutoffs
)

select
    sha2(
        to_json(named_struct(
            'contract_version', '{{ var("contract_version") }}',
            'prediction_timestamp', prediction_timestamp
        )),
        256
    ) as cutoff_id,
    '{{ var("contract_version") }}' as contract_version,
    prediction_timestamp,
    dataset_start_time,
    dataset_end_time,
    cast(
        timestampadd(
            HOUR,
            -{{ var('active_lookback_hours') }},
            prediction_timestamp
        ) >= dataset_start_time
        as boolean
    ) as has_complete_global_lookback,
    cast(
        timestampadd(
            HOUR,
            {{ var('target_horizon_hours') }},
            prediction_timestamp
        ) <= dataset_end_time
        as boolean
    ) as has_complete_global_followup
from cutoffs
