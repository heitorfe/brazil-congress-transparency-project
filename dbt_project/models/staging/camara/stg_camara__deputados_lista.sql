{{ config(materialized='view') }}

-- Grain: 1 row per deputy × legislature
-- Source: data/raw/camara_deputados_lista.parquet
-- Records which legislature(s) each deputy participated in.

with source as (
    select * from read_parquet('../data/raw/camara_deputados_lista.parquet')
),

renamed as (
    select
        cast(deputado_id as varchar)        as deputado_id,
        cast(id_legislatura as integer)     as id_legislatura,
        trim(nome)                          as nome,
        upper(trim(sigla_partido))          as sigla_partido,
        upper(trim(sigla_uf))               as sigla_uf,
        trim(url_foto)                      as url_foto,
        trim(email)                         as email
    from source
    where deputado_id is not null
      and deputado_id != ''
)

select * from renamed
