with expected as (
    select
        (select count(*) from {{ ref('int_scoring_cutoffs') }})
            * (select count(*) from {{ ref('int_equipamentos') }}) as expected_rows
),

actual as (
    select count(*) as actual_rows
    from {{ ref('int_scoring_grade') }}
)

select *
from expected
cross join actual
where expected.expected_rows <> actual.actual_rows
