with primary_observed_episodes as (
    select
        episode_id,
        episode_start_proxy,
        tipo_equipamento
    from {{ ref('int_dontgo_episodios') }}
    where
        is_primary_gap
        and episode_start_is_observed
),

targetable_episodes as (
    select distinct next_episode_id as episode_id
    from {{ ref('int_observacoes_elegiveis') }}
    where y_episode = 1
)

select
    coalesce(date_format(date_trunc('MONTH', episodes.episode_start_proxy), 'yyyy-MM'), 'ALL')
        as episode_month,
    coalesce(episodes.tipo_equipamento, 'ALL') as tipo_equipamento,
    count(*) as observed_episode_count,
    count_if(targetable.episode_id is not null) as targetable_episode_count,
    count_if(targetable.episode_id is null) as non_targetable_episode_count,
    cast(
        count_if(targetable.episode_id is not null) / count(*)
        as decimal(18, 8)
    ) as targetable_episode_rate
from primary_observed_episodes as episodes
left join targetable_episodes as targetable
    on episodes.episode_id = targetable.episode_id
group by grouping sets (
    (date_trunc('MONTH', episodes.episode_start_proxy), episodes.tipo_equipamento),
    (date_trunc('MONTH', episodes.episode_start_proxy)),
    (episodes.tipo_equipamento),
    ()
)
order by episode_month, tipo_equipamento
