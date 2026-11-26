with per_episode as (
    select
        episode_id,
        episode_gap_hours,
        count_if(is_episode_anchor) as anchor_count,
        count(distinct tag) as tag_count,
        count(distinct tipo_equipamento) as equipment_type_count,
        min(event_time) as min_event_time,
        min(episode_anchor_time) as declared_anchor_time
    from {{ ref('int_dontgo_episodio_membros') }}
    group by episode_id, episode_gap_hours
)

select *
from per_episode
where
    anchor_count <> 1
    or tag_count <> 1
    or equipment_type_count <> 1
    or min_event_time <> declared_anchor_time
