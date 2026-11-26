with member_aggregates as (
    select
        episode_id,
        count(*) as expected_occurrence_count,
        min(event_time) as expected_start,
        max(event_time) as expected_end,
        max(case when is_episode_anchor then ocorrencia_id end) as expected_anchor_occurrence_id
    from {{ ref('int_dontgo_episodio_membros') }}
    group by episode_id
),

episodes as (
    select *
    from {{ ref('int_dontgo_episodios') }}
)

select
    coalesce(member_aggregates.episode_id, episodes.episode_id) as episode_id,
    member_aggregates.expected_occurrence_count,
    episodes.occurrence_count,
    member_aggregates.expected_start,
    episodes.episode_start_proxy,
    member_aggregates.expected_end,
    episodes.episode_end,
    member_aggregates.expected_anchor_occurrence_id,
    episodes.anchor_occurrence_id
from member_aggregates
full outer join episodes
    on member_aggregates.episode_id = episodes.episode_id
where
    member_aggregates.episode_id is null
    or episodes.episode_id is null
    or member_aggregates.expected_occurrence_count <> episodes.occurrence_count
    or member_aggregates.expected_start <> episodes.episode_start_proxy
    or member_aggregates.expected_end <> episodes.episode_end
    or member_aggregates.expected_anchor_occurrence_id <> episodes.anchor_occurrence_id
