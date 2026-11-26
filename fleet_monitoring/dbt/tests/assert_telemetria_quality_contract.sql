with audit as (
    select
        count_if(event_time is null or availability_time is null) as null_event_time_rows,
        count_if(inicio_turno is null or fim_turno is null) as null_shift_rows,
        count_if(tag is null or trim(tag) = '') as null_or_blank_tag_rows,
        count_if(tipo_equipamento not in ('Caminhao', 'Escavadeira')) as invalid_type_rows,
        count_if(
            operador_anonimo is null
            or matricula_operador_hash is null
            or alarme_id is null
            or alarme is null
        ) as incomplete_context_rows,
        count_if(criticidade_id not in (1, 2, 3, 4) or criticidade is null) as invalid_criticality_rows,
        count_if(
            criticidade <> case criticidade_id
                when 1 then 'Crítico'
                when 2 then 'Não Crítico'
                when 3 then 'Informacional'
                when 4 then 'Outros'
            end
        ) as inconsistent_criticality_rows,
        count_if(
            valor_raw is null
            or (upper(valor_raw) = 'NULL' and valor is not null)
            or (upper(valor_raw) <> 'NULL' and valor is null)
        ) as invalid_numeric_value_rows,
        count_if(
            classe_raw is null
            or (upper(classe_raw) = 'NULL' and classe is not null)
            or (upper(classe_raw) <> 'NULL' and classe is null)
            or (classe not in ('Activate', 'Inactive') and classe is not null)
        ) as invalid_class_rows,
        count_if(is_dont_go not in (0, 1) or is_dont_go is null) as invalid_target_rows,
        count_if(row_hash is null) as null_hash_rows,
        count_if(
            source_path is null
            or source_sha256 is null
            or source_period is null
            or manifest_version is null
            or ingestion_time is null
        ) as incomplete_lineage_rows
    from {{ ref('stg_telemetria') }}
)

select *
from audit
where
    null_event_time_rows > 0
    or null_shift_rows > 0
    or null_or_blank_tag_rows > 0
    or invalid_type_rows > 0
    or incomplete_context_rows > 0
    or invalid_criticality_rows > 0
    or inconsistent_criticality_rows > 0
    or invalid_numeric_value_rows > 0
    or invalid_class_rows > 0
    or invalid_target_rows > 0
    or null_hash_rows > 0
    or incomplete_lineage_rows > 0
