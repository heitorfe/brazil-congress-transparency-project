{{ config(materialized='view') }}

-- Grain: 1 row per committee (active committees across SF, CN, CD)
-- Source: data/raw/comissoes.parquet
--   Primary: /comissao/lista/colegiados (all active, flat schema)
--   Augmented: /comissao/lista/mistas (joint CN committees + member counts)
-- Responsibility: type casting and normalization only

with source as (
    select * from read_parquet('../data/raw/comissoes.parquet')
),

renamed as (
    select
        cast(codigo_comissao as varchar)         as codigo_comissao,
        upper(trim(sigla_comissao))              as sigla_comissao,
        trim(nome_comissao)                      as nome_comissao,
        trim(finalidade)                         as finalidade,
        upper(trim(sigla_casa))                  as sigla_casa,
        cast(codigo_tipo as varchar)             as codigo_tipo,
        upper(trim(sigla_tipo))                  as sigla_tipo,
        trim(descricao_tipo)                     as descricao_tipo,
        try_cast(data_inicio as date)            as data_inicio,
        try_cast(data_fim as date)               as data_fim,
        cast(publica as boolean)                 as publica,
        cast(qtd_titulares as integer)           as qtd_titulares,
        cast(qtd_senadores_titulares as integer) as qtd_senadores_titulares,
        cast(qtd_deputados_titulares as integer) as qtd_deputados_titulares,
        cast(fonte as varchar)                   as fonte
    from source
    where codigo_comissao is not null
      and trim(codigo_comissao) != ''
)

select * from renamed
