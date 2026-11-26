with population as (
    select *
    from {{ ref('int_scoring_grade') }}
),

funnel as (
    select 1 as step_order, 'grade completa' as step, count(*) as row_count
    from population

    union all

    select 2, 'atividade em [t-4h,t)', count(*)
    from population
    where active_event_count > 0

    union all

    select 3, 'sem Dont Go em [t-4h,t]', count(*)
    from population
    where
        active_event_count > 0
        and washout_occurrence_count = 0

    union all

    select 4, 'lookback global observavel', count(*)
    from population
    where
        active_event_count > 0
        and washout_occurrence_count = 0
        and has_complete_global_lookback
        and not has_lookback_unavailability

    union all

    select 5, 'elegivel para scoring', count(*)
    from population
    where is_scoring_eligible

    union all

    select 6, 'elegivel para treino e label', count(*)
    from population
    where is_training_eligible
)

select
    step_order,
    step,
    row_count,
    lag(row_count) over (order by step_order) - row_count as rows_removed_at_step,
    cast(row_count / first_value(row_count) over (order by step_order) as decimal(18, 8))
        as retained_from_grid_rate
from funnel
order by step_order
