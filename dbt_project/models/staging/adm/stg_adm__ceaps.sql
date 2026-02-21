{{ config(materialized='view') }}

-- Grain: 1 row per expense reimbursement receipt
-- Source: data/raw/ceaps.parquet (ADM API /api/v1/senadores/despesas_ceaps/{ano})
-- Key quirk: cod_senador is an INTEGER in the ADM API — cast to VARCHAR for FK join to dim_senador

with source as (
    select * from read_parquet('../data/raw/ceaps.parquet')
),

renamed as (
    select
        cast(id as bigint)                         as id,
        -- ADM API returns cod_senador as integer; VARCHAR needed for FK join to dim_senador
        cast(cod_senador as varchar)               as senador_id,
        trim(nome_senador)                         as nome_senador,
        cast(ano as integer)                       as ano,
        cast(mes as integer)                       as mes,
        trim(tipo_despesa)                         as tipo_despesa,
        trim(cnpj_cpf)                             as cnpj_cpf,
        trim(fornecedor)                           as fornecedor,
        trim(documento)                            as documento,
        try_cast(data as date)                     as data,
        trim(detalhamento)                         as detalhamento,
        cast(valor_reembolsado as decimal(12, 2))  as valor_reembolsado,
        trim(tipo_documento)                       as tipo_documento
    from source
    where id is not null
      and cod_senador is not null
)

select * from renamed
