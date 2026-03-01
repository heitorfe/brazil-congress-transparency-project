{{ config(materialized='view') }}

-- Grain: 1 row per expense document (cod_documento × deputado_id)
-- Source: data/raw/camara_despesas.parquet
-- Key: expense values are already native floats (unlike Senate CEAPS which uses
--      Brazilian-locale strings). No REPLACE/CAST trick needed here.

with source as (
    select * from read_parquet('../data/raw/camara_despesas.parquet')
),

renamed as (
    select
        trim(cod_documento)                             as cod_documento,
        cast(deputado_id as varchar)                    as deputado_id,
        cast(ano as integer)                            as ano,
        cast(mes as integer)                            as mes,
        trim(tipo_despesa)                              as tipo_despesa,
        cast(cod_tipo_documento as integer)             as cod_tipo_documento,
        trim(tipo_documento)                            as tipo_documento,
        try_cast(data_documento as date)                as data_documento,
        trim(num_documento)                             as num_documento,
        try_cast(valor_documento as decimal(12, 2))     as valor_documento,
        trim(url_documento)                             as url_documento,
        trim(nome_fornecedor)                           as nome_fornecedor,
        trim(cnpj_cpf_fornecedor)                       as cnpj_cpf_fornecedor,
        try_cast(valor_liquido as decimal(12, 2))       as valor_liquido,
        try_cast(valor_glosa as decimal(12, 2))         as valor_glosa,
        trim(num_ressarcimento)                         as num_ressarcimento,
        trim(cod_lote)                                  as cod_lote,
        cast(parcela as integer)                        as parcela
    from source
    where cod_documento is not null
      and cod_documento != ''
      and deputado_id is not null
)

select * from renamed
