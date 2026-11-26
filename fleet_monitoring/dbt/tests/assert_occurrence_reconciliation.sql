with telemetry_positive as (
    select
        count(*) as row_count,
        count(distinct evento_telemetria_id) as distinct_id_count
    from {{ ref('stg_telemetria') }}
    where is_dont_go = 1
),

occurrences as (
    select
        count(*) as row_count,
        count(distinct ocorrencia_id) as distinct_id_count
    from {{ ref('int_dontgo_ocorrencias') }}
)

select
    telemetry_positive.row_count as expected_rows,
    occurrences.row_count as actual_rows,
    occurrences.distinct_id_count as actual_distinct_ids
from telemetry_positive
cross join occurrences
where
    telemetry_positive.row_count <> occurrences.row_count
    or telemetry_positive.distinct_id_count <> occurrences.distinct_id_count
    or occurrences.row_count <> occurrences.distinct_id_count
