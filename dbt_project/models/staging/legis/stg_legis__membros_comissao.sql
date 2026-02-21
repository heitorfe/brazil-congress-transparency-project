{{ config(materialized='view') }}

-- Grain: 1 row per senator × committee × role period
-- Source: data/raw/membros_comissao.parquet (from /senador/{code}/comissoes for all senators)
-- Responsibility: type casting and normalization — no business logic

with source as (
    select * from read_parquet('../data/raw/membros_comissao.parquet')
),

renamed as (
    select
        cast(senador_id as varchar)         as senador_id,
        cast(codigo_comissao as varchar)    as codigo_comissao,
        upper(trim(sigla_comissao))         as sigla_comissao,
        trim(nome_comissao)                 as nome_comissao,
        upper(trim(sigla_casa))             as sigla_casa,
        trim(descricao_participacao)        as descricao_participacao,
        try_cast(data_inicio as date)       as data_inicio,
        try_cast(data_fim as date)          as data_fim
    from source
    where senador_id is not null
      and codigo_comissao is not null
      and data_inicio is not null
)

select * from renamed
