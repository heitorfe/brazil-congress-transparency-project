{{ config(materialized='view') }}

-- Grain: 1 row per unique deputy (biographical data from GET /deputados/{id})
-- Source: data/raw/camara_deputados.parquet
-- Responsibility: type casting and field renaming only — no business logic

with source as (
    select * from read_parquet('../data/raw/camara_deputados.parquet')
),

renamed as (
    select
        cast(deputado_id as varchar)                as deputado_id,
        trim(nome_civil)                            as nome_civil,
        trim(nome_parlamentar)                      as nome_parlamentar,
        trim(nome_eleitoral)                        as nome_eleitoral,
        upper(trim(sigla_partido))                  as sigla_partido,
        upper(trim(sigla_uf))                       as sigla_uf,
        cast(id_legislatura as integer)             as id_legislatura,
        trim(url_foto)                              as url_foto,
        -- email is Null-typed in Parquet when all values are null; cast to varchar first
        trim(try_cast(email as varchar))            as email,
        trim(situacao)                              as situacao,
        trim(condicao_eleitoral)                    as condicao_eleitoral,
        trim(descricao_status)                      as descricao_status,
        try_cast(left(data_status, 10) as date)     as data_status,
        upper(trim(sexo))                           as sexo,
        try_cast(data_nascimento as date)           as data_nascimento,
        upper(trim(uf_nascimento))                  as uf_nascimento,
        trim(municipio_nascimento)                  as municipio_nascimento,
        trim(escolaridade)                          as escolaridade,
        trim(telefone_gabinete)                     as telefone_gabinete
    from source
    where deputado_id is not null
      and deputado_id != ''
)

select * from renamed
