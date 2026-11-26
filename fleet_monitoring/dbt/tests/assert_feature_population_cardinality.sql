with expected as (
    select count(*) as row_count
    from {{ ref('int_scoring_grade') }}
    where is_scoring_eligible
),

actual as (
    select count(*) as row_count
    from {{ ref('fct_features_scoring') }}
)

select
    expected.row_count as expected_row_count,
    actual.row_count as actual_row_count
from expected
cross join actual
where expected.row_count <> actual.row_count
