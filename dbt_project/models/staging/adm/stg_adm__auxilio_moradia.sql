{{ config(materialized='view') }}

-- Grain: 1 row per senator (current snapshot — no date history in ADM API)
-- Source: data/raw/auxilio_moradia.parquet (ADM API /api/v1/senadores/auxilio-moradia)
-- Note: no senator ID available — matched to dim_senador via nome_parlamentar in mart layer

with source as (
    select * from read_parquet('../data/raw/auxilio_moradia.parquet')
),

renamed as (
    select
        upper(trim(nome_parlamentar))   as nome_parlamentar,
        upper(trim(estado_eleito))      as estado_eleito,
        upper(trim(partido_eleito))     as partido_eleito,
        cast(auxilio_moradia as boolean) as auxilio_moradia,
        cast(imovel_funcional as boolean) as imovel_funcional
    from source
    where nome_parlamentar is not null
      and nome_parlamentar != ''
)

select * from renamed
