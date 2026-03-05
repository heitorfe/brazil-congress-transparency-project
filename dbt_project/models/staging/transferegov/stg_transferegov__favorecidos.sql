{{ config(materialized='view', tags=['transferegov']) }}

-- Grain: 1 row per (codigo_emenda × codigo_favorecido × ano_mes)
-- Source: read_parquet('../data/raw/transferegov_favorecidos_*.parquet')
-- NOTE: The portal returns a consolidated all-years snapshot per request;
--   the 6 year parquets (2020-2025) are near-identical. Dedup by favorecido_id.
-- Responsibilities:
--   • BRL parsing for valor_transferido
--   • Supplier type classification (CNPJ=14d / CPF=11d)
--   • Normalize names for cross-reference joins

with source as (
    select * from read_parquet('../data/raw/transferegov_favorecidos_*.parquet')
    where favorecido_id is not null
      and codigo_emenda is not null
),

deduped as (
    select *
    from source
    qualify row_number() over (partition by favorecido_id order by ano desc) = 1
),

renamed as (
    select
        favorecido_id,
        trim(codigo_emenda)              as codigo_emenda,
        try_cast(nullif(trim(ano), '') as integer) as ano,
        trim(codigo_autor_emenda)        as codigo_autor_emenda,
        trim(nome_autor_emenda)          as nome_autor_emenda,
        trim(numero_emenda)              as numero_emenda,
        trim(tipo_emenda)                as tipo_emenda,
        trim(codigo_favorecido)          as codigo_favorecido,
        trim(nome_favorecido)            as nome_favorecido,
        upper(trim(nome_favorecido))     as nome_favorecido_norm,
        trim(natureza_juridica)          as natureza_juridica,
        trim(tipo_pessoa)                as tipo_pessoa,
        trim(municipio_favorecido)       as municipio_favorecido,
        trim(uf_favorecido)              as uf_favorecido,
        -- BRL value parsing
        try_cast(
            replace(replace(trim(valor_transferido_raw), '.', ''), ',', '.')
            as double
        )                                as valor_transferido,
        try_cast(
            replace(replace(trim(valor_empenhado_raw), '.', ''), ',', '.')
            as double
        )                                as valor_empenhado,
        try_cast(
            replace(replace(trim(valor_pago_raw), '.', ''), ',', '.')
            as double
        )                                as valor_pago,
        -- Recipient type by CNPJ/CPF digit count
        case
            when length(regexp_replace(coalesce(trim(codigo_favorecido), ''), '[^0-9]', '', 'g')) = 14
                then 'CNPJ'
            when length(regexp_replace(coalesce(trim(codigo_favorecido), ''), '[^0-9]', '', 'g')) = 11
                then 'CPF'
            else 'unknown'
        end                              as tipo_doc_favorecido
    from deduped
)

select * from renamed
