select
    episodes.episode_id,
    episodes.episode_gap_hours,
    episodes.episode_start_proxy,
    episodes.episode_start_is_observed,
    episodes.episode_start_history_is_unavailable
from {{ ref('int_dontgo_episodios') }} as episodes
inner join {{ ref('int_telemetria_indisponibilidade_global') }} as unavailability
    on unavailability.unavailable_start_exclusive < episodes.episode_start_proxy
    and unavailability.unavailable_end_exclusive > timestampadd(
        HOUR,
        -episodes.episode_gap_hours,
        episodes.episode_start_proxy
    )
where
    episodes.episode_start_is_observed
    or not episodes.episode_start_history_is_unavailable
