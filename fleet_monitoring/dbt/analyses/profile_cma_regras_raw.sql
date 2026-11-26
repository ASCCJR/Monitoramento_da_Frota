with source_data as (
    select *
    from {{ source('bronze_v2', 'cma_regras_raw') }}
),

event_cardinality as (
    select
        trim(EVENTO) as event_name,
        count(*) as rule_count
    from source_data
    group by trim(EVENTO)
)

select
    count(*) as row_count,
    count(distinct named_struct(
        'tipo', trim(TIPO),
        'evento', trim(EVENTO),
        'situacao', trim(SITUACAO),
        'quantidade', QUANTIDADE,
        'tempo', TEMPO,
        'nivel', lower(trim(NIVEL))
    )) as distinct_normalized_rule_count,
    count_if(
        TIPO is null or trim(TIPO) = ''
        or EVENTO is null or trim(EVENTO) = ''
        or SITUACAO is null or trim(SITUACAO) = ''
        or NIVEL is null or trim(NIVEL) = ''
    ) as incomplete_rule_rows,
    count_if(QUANTIDADE is null or QUANTIDADE <= 0) as invalid_quantity_rows,
    count_if(TEMPO is null or TEMPO <= 0) as invalid_time_window_rows,
    min(QUANTIDADE) as min_quantity,
    max(QUANTIDADE) as max_quantity,
    min(TEMPO) as min_time_window,
    max(TEMPO) as max_time_window,
    count(distinct trim(TIPO)) as distinct_equipment_type_count,
    count(distinct trim(EVENTO)) as distinct_event_count,
    count(distinct lower(trim(NIVEL))) as distinct_normalized_level_count,
    count_if(lower(trim(NIVEL)) <> trim(NIVEL)) as level_rows_changed_by_lowercase,
    count(distinct _source_sha256) as source_file_count,
    (select count_if(rule_count > 1) from event_cardinality) as events_with_multiple_rules
from source_data
