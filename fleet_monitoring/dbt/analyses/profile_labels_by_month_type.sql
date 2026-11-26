select
    coalesce(date_format(date_trunc('MONTH', prediction_timestamp), 'yyyy-MM'), 'ALL')
        as prediction_month,
    coalesce(tipo_equipamento, 'ALL') as tipo_equipamento,
    count(*) as eligible_observation_count,
    sum(y_episode) as positive_observation_count,
    cast(avg(y_episode) as decimal(18, 8)) as positive_prevalence,
    count(distinct next_episode_id) as targetable_episode_count,
    cast(avg(lead_time_minutes) as decimal(18, 2)) as mean_positive_lead_minutes,
    cast(percentile_approx(lead_time_minutes, 0.5) as decimal(18, 2))
        as median_positive_lead_minutes
from {{ ref('int_observacoes_elegiveis') }}
group by grouping sets (
    (date_trunc('MONTH', prediction_timestamp), tipo_equipamento),
    (date_trunc('MONTH', prediction_timestamp)),
    (tipo_equipamento),
    ()
)
order by prediction_month, tipo_equipamento
