select
    (select count(*) from {{ ref('int_cma_evento_lookup') }}) as cma_event_name_count,
    (select sum(cma_rule_count) from {{ ref('int_cma_evento_lookup') }})
        as reconciled_cma_rule_count,
    (select count(*) from {{ ref('int_telemetria_features_hora_tag') }})
        as hourly_tag_row_count,
    (select sum(event_count) from {{ ref('int_telemetria_features_hora_tag') }})
        as reconciled_telemetry_event_count,
    (select sum(cma_event_count) from {{ ref('int_telemetria_features_hora_tag') }})
        as cma_associated_event_count,
    (select count(*) from {{ ref('fct_features_scoring') }})
        as scoring_feature_row_count,
    (select count(*) from {{ ref('fct_training_set') }})
        as training_set_row_count
