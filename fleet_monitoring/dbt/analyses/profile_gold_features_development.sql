select
    date_format(date_trunc('MONTH', prediction_timestamp), 'yyyy-MM') as month,
    count(*) as row_count,
    count(distinct tag) as tag_count,
    count_if(history_1h_is_complete) as complete_1h_row_count,
    count_if(history_4h_is_complete) as complete_4h_row_count,
    count_if(history_24h_is_complete) as complete_24h_row_count,
    count_if(episode_history_7d_is_complete) as complete_7d_row_count,
    count_if(episode_history_30d_is_complete) as complete_30d_row_count,
    count_if(has_prior_episode = 1) as rows_with_prior_episode,
    cast(avg(events_4h) as decimal(18, 2)) as mean_events_4h,
    cast(avg(events_24h) as decimal(18, 2)) as mean_events_24h,
    cast(avg(critical_rate_4h) as decimal(18, 8)) as mean_critical_rate_4h,
    cast(avg(cma_rate_4h) as decimal(18, 8)) as mean_cma_rate_4h
from {{ ref('fct_features_scoring') }}
where prediction_timestamp < timestamp_ntz'2025-06-01 00:00:00'
group by date_trunc('MONTH', prediction_timestamp)
order by month
