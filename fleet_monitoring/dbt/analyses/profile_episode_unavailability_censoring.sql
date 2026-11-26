select
    episodes.episode_gap_hours,
    count(distinct episodes.episode_id) as affected_episode_count,
    min(episodes.episode_start_proxy) as first_affected_episode_start,
    max(episodes.episode_start_proxy) as last_affected_episode_start
from {{ ref('int_dontgo_episodios') }} as episodes
inner join {{ ref('int_telemetria_indisponibilidade_global') }} as unavailable
    on unavailable.unavailable_start_exclusive < episodes.episode_start_proxy
    and unavailable.unavailable_end_exclusive > timestampadd(
        HOUR,
        -episodes.episode_gap_hours,
        episodes.episode_start_proxy
    )
where episodes.episode_start_is_observed
group by episodes.episode_gap_hours
order by episodes.episode_gap_hours
