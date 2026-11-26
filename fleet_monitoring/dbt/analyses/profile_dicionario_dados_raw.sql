with source_data as (
    select *
    from {{ source('bronze_v2', 'dicionario_dados_raw') }}
)

select
    count(*) as row_count,
    count(distinct named_struct(
        'table_name', lower(trim(nome_tabela)),
        'feature_name', lower(trim(nome_feature))
    )) as distinct_table_feature_count,
    count_if(nome_tabela is null or trim(nome_tabela) = '') as null_or_blank_table_rows,
    count_if(nome_feature is null or trim(nome_feature) = '') as null_or_blank_feature_rows,
    count_if(descricao is null or trim(descricao) = '') as null_or_blank_description_rows,
    count(distinct lower(trim(nome_tabela))) as documented_table_count,
    count(distinct _source_sheet) as source_sheet_count,
    count(distinct _source_sha256) as source_file_count
from source_data
