with source_data as (
    select *
    from {{ source('bronze_v2', 'telemetria_raw') }}
),

renamed as (
    select
        cast(Id_Eventos_Telemetria as bigint) as evento_telemetria_id,
        cast(Data_Evento as timestamp_ntz) as event_time,
        cast(Data_Evento as timestamp_ntz) as availability_time,
        cast(true as boolean) as availability_is_proxy,
        try_cast(Inicio_Turno as timestamp_ntz) as inicio_turno,
        try_cast(Fim_Turno as timestamp_ntz) as fim_turno,
        cast(Dia as int) as dia_mes_origem,
        trim(Localidade) as localidade,
        trim(TAG) as tag,
        trim(Tag_Frota) as tag_frota,
        trim(Tipo) as tipo_equipamento,
        trim(Nome_Operador_Anon) as operador_anonimo,
        trim(Matricula_Operador_Hash) as matricula_operador_hash,
        cast(Id_Alarme as bigint) as alarme_id,
        trim(Alarme) as alarme,
        cast(Id_Criticidade as int) as criticidade_id,
        trim(Criticidade) as criticidade_raw,
        case cast(Id_Criticidade as int)
            when 1 then 'Crítico'
            when 2 then 'Não Crítico'
            when 3 then 'Informacional'
            when 4 then 'Outros'
        end as criticidade,
        trim(Valor) as valor_raw,
        case
            when upper(trim(Valor)) = 'NULL' then cast(null as double)
            else try_cast(replace(trim(Valor), ',', '.') as double)
        end as valor,
        trim(Classe) as classe_raw,
        case
            when upper(trim(Classe)) = 'NULL' then cast(null as string)
            else trim(Classe)
        end as classe,
        cast(Is_Dont_Go as tinyint) as is_dont_go,
        sha2(
            to_json(named_struct(
                'evento_telemetria_id', Id_Eventos_Telemetria,
                'event_time', Data_Evento,
                'tag', TAG,
                'alarme_id', Id_Alarme,
                'criticidade_id', Id_Criticidade,
                'valor_raw', Valor,
                'classe_raw', Classe,
                'is_dont_go', Is_Dont_Go
            )),
            256
        ) as row_hash,
        _source_path as source_path,
        _source_sha256 as source_sha256,
        _source_period as source_period,
        _manifest_version as manifest_version,
        cast(_ingested_at_utc as timestamp) as ingestion_time
    from source_data
)

select
    evento_telemetria_id,
    event_time,
    availability_time,
    availability_is_proxy,
    inicio_turno,
    fim_turno,
    dia_mes_origem,
    localidade,
    tag,
    tag_frota,
    tipo_equipamento,
    operador_anonimo,
    matricula_operador_hash,
    alarme_id,
    alarme,
    criticidade_id,
    criticidade_raw,
    criticidade,
    valor_raw,
    valor,
    classe_raw,
    classe,
    is_dont_go,
    row_hash,
    source_path,
    source_sha256,
    source_period,
    manifest_version,
    ingestion_time
from renamed
