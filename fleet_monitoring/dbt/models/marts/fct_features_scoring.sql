with eligible_grid as (
    select *
    from {{ ref('int_scoring_grade') }}
    where is_scoring_eligible
),

hourly_features as (
    select *
    from {{ ref('int_telemetria_features_hora_tag') }}
),

observed_primary_episodes as (
    select *
    from {{ ref('int_dontgo_episodios') }}
    where
        is_primary_gap
        and episode_start_is_observed
),

unavailability as (
    select *
    from {{ ref('int_telemetria_indisponibilidade_global') }}
),

window_unavailability as (
    select
        grid.scoring_row_id,
        count_if(
            unavailability.unavailable_start_exclusive < grid.prediction_timestamp
            and unavailability.unavailable_end_exclusive > timestampadd(
                HOUR,
                -24,
                grid.prediction_timestamp
            )
        ) > 0 as history_24h_has_unavailability,
        count_if(
            unavailability.unavailable_start_exclusive < grid.prediction_timestamp
            and unavailability.unavailable_end_exclusive > timestampadd(
                DAY,
                -7,
                grid.prediction_timestamp
            )
        ) > 0 as episode_history_7d_has_unavailability,
        count_if(
            unavailability.unavailable_start_exclusive < grid.prediction_timestamp
            and unavailability.unavailable_end_exclusive > timestampadd(
                DAY,
                -30,
                grid.prediction_timestamp
            )
        ) > 0 as episode_history_30d_has_unavailability
    from eligible_grid as grid
    left join unavailability
        on unavailability.unavailable_start_exclusive < grid.prediction_timestamp
        and unavailability.unavailable_end_exclusive > timestampadd(
            DAY,
            -30,
            grid.prediction_timestamp
        )
    group by grid.scoring_row_id
),

telemetry_windows as (
    select
        grid.scoring_row_id,
        coalesce(sum(case
            when hourly.event_hour >= timestampadd(HOUR, -1, grid.prediction_timestamp)
                then hourly.event_count
            else 0
        end), 0) as events_1h,
        coalesce(sum(case
            when hourly.event_hour >= timestampadd(HOUR, -4, grid.prediction_timestamp)
                then hourly.event_count
            else 0
        end), 0) as events_4h,
        coalesce(sum(hourly.event_count), 0) as events_24h,
        coalesce(sum(case
            when hourly.event_hour >= timestampadd(HOUR, -4, grid.prediction_timestamp)
                then hourly.critical_event_count
            else 0
        end), 0) as critical_events_4h,
        coalesce(sum(hourly.critical_event_count), 0) as critical_events_24h,
        coalesce(sum(case
            when hourly.event_hour >= timestampadd(HOUR, -4, grid.prediction_timestamp)
                then hourly.noncritical_event_count
            else 0
        end), 0) as noncritical_events_4h,
        coalesce(sum(hourly.noncritical_event_count), 0) as noncritical_events_24h,
        coalesce(sum(case
            when hourly.event_hour >= timestampadd(HOUR, -4, grid.prediction_timestamp)
                then hourly.informational_event_count
            else 0
        end), 0) as informational_events_4h,
        coalesce(sum(hourly.informational_event_count), 0) as informational_events_24h,
        coalesce(sum(case
            when hourly.event_hour >= timestampadd(HOUR, -4, grid.prediction_timestamp)
                then hourly.other_event_count
            else 0
        end), 0) as other_events_4h,
        coalesce(sum(hourly.other_event_count), 0) as other_events_24h,
        coalesce(sum(case
            when hourly.event_hour >= timestampadd(HOUR, -4, grid.prediction_timestamp)
                then hourly.cma_event_count
            else 0
        end), 0) as cma_events_4h,
        coalesce(sum(hourly.cma_event_count), 0) as cma_events_24h,
        coalesce(sum(case
            when hourly.event_hour >= timestampadd(HOUR, -4, grid.prediction_timestamp)
                then hourly.cma_very_high_event_count
            else 0
        end), 0) as cma_very_high_events_4h,
        coalesce(sum(hourly.cma_very_high_event_count), 0)
            as cma_very_high_events_24h,
        coalesce(sum(case
            when hourly.event_hour >= timestampadd(HOUR, -4, grid.prediction_timestamp)
                then hourly.cma_high_event_count
            else 0
        end), 0) as cma_high_events_4h,
        coalesce(sum(hourly.cma_high_event_count), 0) as cma_high_events_24h,
        coalesce(sum(hourly.dontgo_occurrence_count), 0) as dontgo_occurrences_24h,
        min(hourly.min_feature_event_time) as min_feature_event_time_24h,
        max(hourly.max_feature_event_time) as max_feature_event_time_24h,
        max(hourly.max_feature_availability_time) as max_feature_availability_time_24h
    from eligible_grid as grid
    left join hourly_features as hourly
        on grid.tag = hourly.tag
        and hourly.event_hour >= timestampadd(HOUR, -24, grid.prediction_timestamp)
        and hourly.event_hour < grid.prediction_timestamp
    group by grid.scoring_row_id
),

episode_windows as (
    select
        grid.scoring_row_id,
        count(episodes.episode_id) as prior_episode_count,
        count_if(
            episodes.episode_start_proxy >= timestampadd(
                DAY,
                -7,
                grid.prediction_timestamp
            )
        ) as episodes_7d,
        count_if(
            episodes.episode_start_proxy >= timestampadd(
                DAY,
                -30,
                grid.prediction_timestamp
            )
        ) as episodes_30d,
        max(episodes.episode_start_proxy) as last_episode_start
    from eligible_grid as grid
    left join observed_primary_episodes as episodes
        on grid.tag = episodes.tag
        and episodes.episode_start_proxy < grid.prediction_timestamp
    group by grid.scoring_row_id
),

base_features as (
    select
        grid.*,
        window_unavailability.history_24h_has_unavailability,
        window_unavailability.episode_history_7d_has_unavailability,
        window_unavailability.episode_history_30d_has_unavailability,
        telemetry_windows.* except (scoring_row_id),
        episode_windows.* except (scoring_row_id)
    from eligible_grid as grid
    inner join window_unavailability
        on grid.scoring_row_id = window_unavailability.scoring_row_id
    inner join telemetry_windows
        on grid.scoring_row_id = telemetry_windows.scoring_row_id
    inner join episode_windows
        on grid.scoring_row_id = episode_windows.scoring_row_id
)

select
    scoring_row_id,
    contract_version,
    '1.0.0' as feature_set_version,
    cutoff_id,
    prediction_timestamp,
    equipamento_id,
    tag,
    tipo_equipamento,
    tag_frota,
    localidade,
    is_training_eligible,
    cast(hour(prediction_timestamp) as int) as cutoff_hour,
    cast(weekday(prediction_timestamp) + 1 as int) as day_of_week_iso,
    cast(dayofmonth(prediction_timestamp) as int) as day_of_month,
    cast(
        hour(prediction_timestamp) < 6 or hour(prediction_timestamp) >= 18
        as tinyint
    ) as is_night_window_proxy,
    cast(tipo_equipamento = 'Caminhao' as tinyint) as is_truck,
    cast(tag_frota = '793-D 2S' as tinyint) as is_fleet_793_d_2s,
    cast(tag_frota = '793-D 3S' as tinyint) as is_fleet_793_d_3s,
    cast(tag_frota = '793-D 4S' as tinyint) as is_fleet_793_d_4s,
    cast(tag_frota = '793-D 5S' as tinyint) as is_fleet_793_d_5s,
    cast(tag_frota = 'LeTourneau L 1850' as tinyint)
        as is_fleet_letourneau_l_1850,
    cast(
        timestampadd(HOUR, -1, prediction_timestamp)
            >= greatest(dataset_start_time, tag_first_event_time)
        and not has_lookback_unavailability
        as boolean
    ) as history_1h_is_complete,
    cast(
        timestampadd(HOUR, -4, prediction_timestamp)
            >= greatest(dataset_start_time, tag_first_event_time)
        and not has_lookback_unavailability
        as boolean
    ) as history_4h_is_complete,
    cast(
        timestampadd(HOUR, -24, prediction_timestamp)
            >= greatest(dataset_start_time, tag_first_event_time)
        and not history_24h_has_unavailability
        as boolean
    ) as history_24h_is_complete,
    cast(
        timestampadd(DAY, -7, prediction_timestamp)
            >= greatest(dataset_start_time, tag_first_event_time)
        and not episode_history_7d_has_unavailability
        as boolean
    ) as episode_history_7d_is_complete,
    cast(
        timestampadd(DAY, -30, prediction_timestamp)
            >= greatest(dataset_start_time, tag_first_event_time)
        and not episode_history_30d_has_unavailability
        as boolean
    ) as episode_history_30d_is_complete,
    events_1h,
    events_4h,
    events_24h,
    critical_events_4h,
    critical_events_24h,
    noncritical_events_4h,
    noncritical_events_24h,
    informational_events_4h,
    informational_events_24h,
    other_events_4h,
    other_events_24h,
    cma_events_4h,
    cma_events_24h,
    cma_very_high_events_4h,
    cma_very_high_events_24h,
    cma_high_events_4h,
    cma_high_events_24h,
    dontgo_occurrences_24h,
    cast(critical_events_4h / nullif(cast(events_4h as double), 0.0) as double)
        as critical_rate_4h,
    cast(critical_events_24h / nullif(cast(events_24h as double), 0.0) as double)
        as critical_rate_24h,
    cast(cma_events_4h / nullif(cast(events_4h as double), 0.0) as double)
        as cma_rate_4h,
    cast(cma_events_24h / nullif(cast(events_24h as double), 0.0) as double)
        as cma_rate_24h,
    cast(events_4h / 4.0 as double) as events_per_hour_4h,
    cast(events_24h / 24.0 as double) as events_per_hour_24h,
    cast(
        (events_4h / 4.0) / nullif(events_24h / 24.0, 0.0)
        as double
    ) as activity_acceleration_4h_vs_24h,
    prior_episode_count,
    episodes_7d,
    episodes_30d,
    case
        when last_episode_start is not null
            then cast(
                timestampdiff(
                    MICROSECOND,
                    last_episode_start,
                    prediction_timestamp
                ) / 3600000000.0
                as double
            )
    end as hours_since_last_episode_start,
    cast(last_episode_start is not null as tinyint) as has_prior_episode,
    cast(
        timestampdiff(
            MICROSECOND,
            greatest(dataset_start_time, tag_first_event_time),
            prediction_timestamp
        ) / 86400000000.0
        as double
    ) as episode_history_exposure_days,
    min_feature_event_time_24h,
    max_feature_event_time_24h,
    max_feature_availability_time_24h,
    history_24h_has_unavailability,
    episode_history_7d_has_unavailability,
    episode_history_30d_has_unavailability,
    dataset_start_time,
    dataset_end_time,
    tag_first_event_time,
    tag_last_event_time
from base_features
