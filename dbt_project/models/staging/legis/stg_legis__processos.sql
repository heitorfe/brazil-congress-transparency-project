{{ config(materialized='view') }}

-- Grain: 1 row per legislative proposal
-- Source: data/raw/processos.parquet (from /processo?sigla=PL/PEC/PLP/MPV&ano=... from 2019)
-- Note: tramitando arrives as "Sim"/"Não" string — converted to boolean here

with source as (
    select * from read_parquet('../data/raw/processos.parquet')
),

renamed as (
    select
        cast(id_processo as bigint)                     as id_processo,
        cast(codigo_materia as bigint)                  as codigo_materia,
        trim(identificacao)                             as identificacao,
        upper(trim(sigla_materia))                      as sigla_materia,
        trim(numero_materia)                            as numero_materia,
        cast(ano_materia as integer)                    as ano_materia,
        trim(ementa)                                    as ementa,
        trim(tipo_documento)                            as tipo_documento,
        try_cast(data_apresentacao as date)             as data_apresentacao,
        trim(autoria)                                   as autoria,
        upper(trim(casa_identificadora))                as casa_identificadora,
        tramitando = 'Sim'                              as tramitando,
        try_cast(data_ultima_atualizacao as timestamp)  as data_ultima_atualizacao,
        trim(url_documento)                             as url_documento
    from source
    where id_processo is not null
)

select * from renamed
