with telemetry_bins as (
    select
        window(event_time, '5 minutes').start as bin_start,
        min(event_time) as first_event_time,
        max(event_time) as last_event_time,
        min(source_period) as first_source_period,
        max(source_period) as last_source_period,
        count(*) as event_count,
        count(distinct tag) as tag_count
    from {{ ref('stg_telemetria') }}
    group by window(event_time, '5 minutes')
),

with_previous_nonempty_bin as (
    select
        *,
        lag(bin_start) over (
            order by bin_start
        ) as previous_bin_start,
        lag(last_event_time) over (
            order by bin_start
        ) as previous_event_time,
        lag(last_source_period) over (
            order by bin_start
        ) as previous_source_period
    from telemetry_bins
),

candidate_gaps as (
    select
        previous_event_time as unavailable_start_exclusive,
        first_event_time as unavailable_end_exclusive,
        timestampdiff(
            SECOND,
            previous_event_time,
            first_event_time
        ) as gap_duration_seconds,
        greatest(
            cast(timestampdiff(MINUTE, previous_bin_start, bin_start) / 5 as int) - 1,
            0
        ) as zero_full_5m_bin_count,
        previous_source_period,
        first_source_period as next_source_period
    from with_previous_nonempty_bin
    where
        previous_event_time is not null
        and timestampdiff(SECOND, previous_event_time, first_event_time)
            >= {{ var('global_unavailability_gap_seconds') }}
),

classified as (
    select
        *,
        case
            when datediff(
                date(unavailable_end_exclusive),
                date(unavailable_start_exclusive)
            ) > 1
                then 'MISSING_CALENDAR_DAY'
            else 'GLOBAL_CAPTURE_GAP'
        end as unavailability_reason
    from candidate_gaps
)

select
    sha2(
        to_json(named_struct(
            'contract_version', '{{ var("contract_version") }}',
            'threshold_seconds', {{ var('global_unavailability_gap_seconds') }},
            'unavailable_start_exclusive', unavailable_start_exclusive,
            'unavailable_end_exclusive', unavailable_end_exclusive
        )),
        256
    ) as unavailability_id,
    '{{ var("contract_version") }}' as contract_version,
    cast({{ var('global_unavailability_gap_seconds') }} as int) as threshold_seconds,
    unavailable_start_exclusive,
    unavailable_end_exclusive,
    gap_duration_seconds,
    zero_full_5m_bin_count,
    previous_source_period,
    next_source_period,
    unavailability_reason,
    cast(gap_duration_seconds >= 14400 as boolean) as invalidates_entire_4h_window
from classified
