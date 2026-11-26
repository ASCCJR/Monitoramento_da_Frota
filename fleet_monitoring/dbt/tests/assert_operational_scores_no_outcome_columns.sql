select column_name
from `{{ target.catalog }}`.information_schema.columns
where table_schema = '{{ env_var('FLEET_GOLD_SCHEMA', 'fleet_gold') }}'
    and table_name = 'fct_operational_scores'
    and lower(column_name) in (
        'y_episode', 'next_episode_id', 'next_episode_start',
        'lead_time_minutes', 'matched_episode_id', 'is_true_alert'
    )
