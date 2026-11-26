with source_data as (
    select *
    from {{ source('bronze_v2', 'dicionario_dados_raw') }}
),

normalized as (
    select
        trim(nome_tabela) as nome_tabela,
        trim(nome_feature) as nome_feature,
        trim(tipo_dados) as tipo_dados,
        trim(descricao) as descricao,
        trim(_source_sheet) as source_sheet,
        _source_path as source_path,
        _source_sha256 as source_sha256,
        _source_period as source_period,
        _manifest_version as manifest_version,
        cast(_ingested_at_utc as timestamp) as ingestion_time
    from source_data
),

identified as (
    select
        sha2(
            to_json(named_struct(
                'nome_tabela', lower(nome_tabela),
                'nome_feature', lower(nome_feature)
            )),
            256
        ) as dicionario_campo_id,
        *
    from normalized
)

select
    dicionario_campo_id,
    nome_tabela,
    nome_feature,
    tipo_dados,
    descricao,
    source_sheet,
    source_path,
    source_sha256,
    source_period,
    manifest_version,
    ingestion_time
from identified
