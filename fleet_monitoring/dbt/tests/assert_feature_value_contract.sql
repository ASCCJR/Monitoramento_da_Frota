select *
from {{ ref('fct_features_scoring') }}
where
    cutoff_hour not between 0 and 23
    or day_of_week_iso not between 1 and 7
    or day_of_month not between 1 and 31
    or is_night_window_proxy not in (0, 1)
    or is_truck not in (0, 1)
    or is_fleet_793_d_2s
        + is_fleet_793_d_3s
        + is_fleet_793_d_4s
        + is_fleet_793_d_5s
        + is_fleet_letourneau_l_1850 <> 1
    or events_1h < 0
    or events_4h < events_1h
    or events_24h < events_4h
    or critical_events_4h > events_4h
    or critical_events_24h > events_24h
    or cma_events_4h > events_4h
    or cma_events_24h > events_24h
    or cma_very_high_events_4h > cma_events_4h
    or cma_very_high_events_24h > cma_events_24h
    or cma_high_events_4h > cma_events_4h
    or cma_high_events_24h > cma_events_24h
    or dontgo_occurrences_24h > events_24h
    or critical_rate_4h not between 0.0 and 1.0
    or critical_rate_24h not between 0.0 and 1.0
    or cma_rate_4h not between 0.0 and 1.0
    or cma_rate_24h not between 0.0 and 1.0
    or episodes_7d > episodes_30d
    or episodes_30d > prior_episode_count
    or has_prior_episode not in (0, 1)
    or (has_prior_episode = 0 and hours_since_last_episode_start is not null)
    or (has_prior_episode = 1 and hours_since_last_episode_start is null)
