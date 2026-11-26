with typed as (
    select
        freeze_id,
        model_name,
        model_version,
        feature_set_version,
        feature_name,
        cast(importance_gain as double) as importance_gain,
        cast(importance_normalized as double) as importance_normalized,
        cast(importance_rank as int) as importance_rank,
        extracted_at_utc
    from {{ source('v2_ml_outputs', 'model_feature_importance') }}
)

select
    *,
    case
        when feature_name in ('cutoff_hour', 'day_of_week_iso', 'day_of_month', 'is_night_window_proxy')
            then 'Calendario'
        when feature_name = 'is_truck' or feature_name like 'is_fleet_%'
            then 'Equipamento'
        when feature_name like 'history_%_is_complete'
            or feature_name like 'episode_history_%'
            or feature_name = 'episode_history_exposure_days'
            then 'Completude'
        when feature_name in (
            'prior_episode_count', 'episodes_7d', 'episodes_30d',
            'hours_since_last_episode_start', 'has_prior_episode'
        )
            then 'Historico de episodios'
        when feature_name like 'cma_%' or feature_name = 'cma_events_4h'
            or feature_name = 'cma_events_24h'
            then 'Associacao CMA'
        else 'Atividade e criticidade'
    end as feature_group
from typed
