{{ config(materialized='view') }}

-- Grain: 1 row per Chamber CEAP expense reimbursement receipt
-- Source: data/raw/ceap_camara_*.parquet (bulk ZIP/CSV 2009–present)
-- Key: expense_id (SHA256[:16] composite, generated in extractor)
-- Join: deputado_id (bigint) → dim_deputado.deputado_id

with source as (
    select * from read_parquet('../data/raw/ceap_camara_*.parquet')
),

renamed as (
    select
        expense_id,
        try_cast(nullif(trim(ano), '') as integer)          as ano,
        try_cast(nullif(trim(mes), '') as integer)          as mes,
        try_cast(nullif(trim(deputado_id), '') as bigint)   as deputado_id,
        trim(nome_parlamentar)                              as nome_parlamentar,
        trim(cpf)                                           as cpf,
        trim(uf)                                            as uf,
        trim(partido_sigla)                                 as partido_sigla,
        try_cast(nullif(trim(cod_legislatura), '') as integer) as cod_legislatura,
        try_cast(nullif(trim(num_sub_cota), '') as integer) as num_sub_cota,
        trim(descricao)                                     as tipo_despesa,
        trim(descricao_especificacao)                       as descricao_especificacao,
        trim(fornecedor)                                    as fornecedor,
        trim(cnpj_cpf)                                      as cnpj_cpf,
        trim(tipo_fornecedor)                               as tipo_fornecedor,
        trim(numero_documento)                              as numero_documento,
        try_cast(nullif(trim(data), '') as date)            as data,

        -- Parse BRL locale strings: "1.234,56" → 1234.56
        try_cast(
            replace(replace(valor_documento, '.', ''), ',', '.')
            as decimal(12, 2)
        )                                                   as valor_documento,
        try_cast(
            replace(replace(valor_glosa, '.', ''), ',', '.')
            as decimal(12, 2)
        )                                                   as valor_glosa,
        try_cast(
            replace(replace(valor_liquido, '.', ''), ',', '.')
            as decimal(12, 2)
        )                                                   as valor_liquido

    from source
    where expense_id is not null
      and deputado_id is not null
      and trim(deputado_id) not in ('', '0')
)

select * from renamed
