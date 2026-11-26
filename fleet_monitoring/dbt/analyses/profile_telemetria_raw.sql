with source_data as (
    select *
    from {{ source('bronze_v2', 'telemetria_raw') }}
),

tag_type_cardinality as (
    select
        TAG,
        count(distinct Tipo) as equipment_type_count
    from source_data
    group by TAG
)

select
    count(*) as row_count,
    count(distinct Id_Eventos_Telemetria) as distinct_event_id_count,
    count(*) - count(distinct Id_Eventos_Telemetria) as duplicate_event_id_rows,
    count_if(Id_Eventos_Telemetria is null) as null_event_id_rows,
    count_if(Data_Evento is null) as null_event_time_rows,
    count_if(TAG is null or trim(TAG) = '') as null_or_blank_tag_rows,
    count_if(Is_Dont_Go not in (0, 1) or Is_Dont_Go is null) as invalid_target_rows,
    count_if(Is_Dont_Go = 1) as dont_go_occurrence_rows,
    count_if(upper(trim(Valor)) = 'NULL') as literal_null_value_rows,
    count_if(upper(trim(Classe)) = 'NULL') as literal_null_class_rows,
    count_if(
        upper(trim(Valor)) <> 'NULL'
        and try_cast(replace(trim(Valor), ',', '.') as double) is null
    ) as invalid_numeric_value_rows,
    count_if(try_cast(Inicio_Turno as timestamp_ntz) is null) as invalid_shift_start_rows,
    count_if(try_cast(Fim_Turno as timestamp_ntz) is null) as invalid_shift_end_rows,
    min(Data_Evento) as min_event_time,
    max(Data_Evento) as max_event_time,
    count(distinct TAG) as distinct_tag_count,
    count(distinct Tipo) as distinct_equipment_type_count,
    count(distinct Localidade) as distinct_location_count,
    count(distinct Id_Alarme) as distinct_alarm_id_count,
    count(distinct Id_Criticidade) as distinct_criticality_id_count,
    count(distinct _source_sha256) as source_file_count,
    (select count_if(equipment_type_count > 1) from tag_type_cardinality) as tags_with_multiple_types
from source_data
