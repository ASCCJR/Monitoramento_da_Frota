with per_occurrence as (
    select
        ocorrencia_id,
        count(*) as membership_count,
        count(distinct episode_gap_hours) as distinct_gap_count
    from {{ ref('int_dontgo_episodio_membros') }}
    group by ocorrencia_id
),

invalid_membership as (
    select *
    from per_occurrence
    where
        membership_count <> {{ var('episode_gap_hours') | length }}
        or distinct_gap_count <> {{ var('episode_gap_hours') | length }}
)

select *
from invalid_membership
