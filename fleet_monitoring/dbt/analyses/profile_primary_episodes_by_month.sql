select
    date_trunc('MONTH', episode_start_proxy) as episode_month,
    tipo_equipamento,
    count(*) as episode_count,
    count_if(episode_start_is_observed) as observed_episode_count,
    sum(occurrence_count) as occurrence_count
from {{ ref('int_dontgo_episodios') }}
where is_primary_gap
group by date_trunc('MONTH', episode_start_proxy), tipo_equipamento
order by episode_month, tipo_equipamento
