{{ config(materialized='view') }}

-- Grain: 1 row per mandate period per senator
-- Source: data/raw/mandatos.parquet
-- A senator may appear in multiple rows (e.g. a senator elected twice)

with source as (
    select * from read_parquet('../data/raw/mandatos.parquet')
),

renamed as (
    select
        senador_id,
        mandato_id,
        estado_sigla,
        try_cast(data_inicio as date)  as mandato_inicio,
        try_cast(data_fim as date)     as mandato_fim,
        descricao_participacao,
        legislatura_inicio,
        legislatura_fim
    from source
    where senador_id is not null
      and senador_id != ''
)

select * from renamed
