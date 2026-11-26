with audit as (
    select
        count_if(event_time is null or inicio is null or fim is null) as null_time_rows,
        count_if(availability_time is null or availability_is_proxy is null) as null_availability_rows,
        count_if(tag is null or trim(tag) = '') as null_or_blank_tag_rows,
        count_if(frota is null or trim(frota) = '') as null_or_blank_fleet_rows,
        count_if(tipo_equipamento is null) as null_type_rows,
        count_if(classe_operacional is null) as null_class_rows,
        count_if(row_hash is null) as null_hash_rows,
        count_if(
            source_path is null
            or source_sha256 is null
            or manifest_version is null
            or ingestion_time is null
        ) as incomplete_lineage_rows
    from {{ ref('stg_apontamentos') }}
)

select *
from audit
where
    null_time_rows > 0
    or null_availability_rows > 0
    or null_or_blank_tag_rows > 0
    or null_or_blank_fleet_rows > 0
    or null_type_rows > 0
    or null_class_rows > 0
    or null_hash_rows > 0
    or incomplete_lineage_rows > 0
