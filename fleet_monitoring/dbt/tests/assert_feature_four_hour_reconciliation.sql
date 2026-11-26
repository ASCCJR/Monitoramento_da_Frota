select
    features.scoring_row_id,
    features.events_4h,
    grid.active_event_count
from {{ ref('fct_features_scoring') }} as features
inner join {{ ref('int_scoring_grade') }} as grid
    on features.scoring_row_id = grid.scoring_row_id
where features.events_4h <> grid.active_event_count
