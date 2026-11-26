select
    episode_gap_hours,
    count(*) as episode_count,
    count_if(episode_start_is_observed) as observed_episode_count,
    count_if(episode_is_left_censored) as left_censored_episode_count,
    count_if(episode_start_history_is_unavailable)
        as unavailability_censored_episode_count,
    sum(occurrence_count) as occurrence_membership_count,
    count(distinct tag) as tag_count,
    min(episode_start_proxy) as min_episode_start_proxy,
    max(episode_start_proxy) as max_episode_start_proxy,
    percentile_approx(occurrence_count, 0.5) as median_occurrence_count,
    max(occurrence_count) as max_occurrence_count
from {{ ref('int_dontgo_episodios') }}
group by episode_gap_hours
order by episode_gap_hours
