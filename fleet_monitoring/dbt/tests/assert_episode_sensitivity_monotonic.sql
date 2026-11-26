with counts_by_gap as (
    select
        episode_gap_hours,
        count(*) as episode_count
    from {{ ref('int_dontgo_episodios') }}
    group by episode_gap_hours
),

ordered as (
    select
        *,
        lag(episode_count) over (
            order by episode_gap_hours
        ) as previous_smaller_gap_count
    from counts_by_gap
)

select *
from ordered
where episode_count > previous_smaller_gap_count
