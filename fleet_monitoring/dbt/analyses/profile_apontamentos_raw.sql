with source_data as (
    select *
    from {{ source('bronze_v2', 'apontamentos_raw') }}
),

ordered_intervals as (
    select
        *,
        max(Fim) over (
            partition by Tag
            order by Inicio, Fim, Id
            rows between unbounded preceding and 1 preceding
        ) as prior_max_end
    from source_data
)

select
    count(*) as row_count,
    count(distinct Id) as distinct_id_count,
    count(*) - count(distinct Id) as duplicate_id_rows,
    count_if(Id is null) as null_id_rows,
    count_if(Tag is null or trim(Tag) = '') as null_or_blank_tag_rows,
    count_if(Inicio is null) as null_start_rows,
    count_if(Fim is null) as null_end_rows,
    count_if(Fim < Inicio) as negative_interval_rows,
    count_if(Fim = Inicio) as zero_duration_rows,
    count_if(Inicio < prior_max_end) as overlapping_interval_rows,
    min(Inicio) as min_start,
    max(Fim) as max_end,
    count(distinct Tag) as distinct_tag_count,
    count(distinct Tipo) as distinct_equipment_type_count,
    count(distinct Classe) as distinct_operational_class_count,
    count(distinct _source_sha256) as source_file_count
from ordered_intervals
