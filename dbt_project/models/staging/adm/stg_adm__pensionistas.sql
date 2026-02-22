{{ config(materialized='view') }}

-- Grain: 1 row per Senate pensioner (current snapshot)
-- Source: data/raw/pensionistas.parquet (ADM API /api/v1/servidores/pensionistas)
-- Nested objects (cargo, funcao, categoria) are pre-flattened during extraction.

with source as (
    select * from read_parquet('../data/raw/pensionistas.parquet')
),

renamed as (
    select
        cast(sequencial as bigint)          as sequencial,
        trim(nome)                          as nome,
        trim(vinculo)                       as vinculo,
        trim(fundamento)                    as fundamento,
        trim(cargo_nome)                    as cargo_nome,
        trim(funcao_nome)                   as funcao_nome,
        trim(categoria_codigo)              as categoria_codigo,
        trim(categoria_nome)                as categoria_nome,
        trim(nome_instituidor)              as nome_instituidor,
        cast(ano_exercicio as integer)      as ano_exercicio,
        try_cast(data_obito as date)        as data_obito,
        try_cast(data_inicio_pensao as date) as data_inicio_pensao
    from source
    where sequencial is not null
)

select * from renamed
