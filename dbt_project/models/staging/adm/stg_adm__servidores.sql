{{ config(materialized='view') }}

-- Grain: 1 row per Senate staff member (current snapshot)
-- Source: data/raw/servidores.parquet (ADM API /api/v1/servidores/servidores)
-- Nested objects (cargo, lotacao, categoria, funcao, cedido) are pre-flattened
-- during extraction. This model only type-casts and trims strings.

with source as (
    select * from read_parquet('../data/raw/servidores.parquet')
),

renamed as (
    select
        cast(sequencial as bigint)    as sequencial,
        trim(nome)                    as nome,
        trim(vinculo)                 as vinculo,
        trim(situacao)                as situacao,
        trim(cargo_nome)              as cargo_nome,
        trim(padrao)                  as padrao,
        trim(especialidade)           as especialidade,
        trim(funcao_nome)             as funcao_nome,
        trim(lotacao_sigla)           as lotacao_sigla,
        trim(lotacao_nome)            as lotacao_nome,
        trim(categoria_codigo)        as categoria_codigo,
        trim(categoria_nome)          as categoria_nome,
        trim(cedido_tipo)             as cedido_tipo,
        trim(cedido_orgao_origem)     as cedido_orgao_origem,
        trim(cedido_orgao_destino)    as cedido_orgao_destino,
        cast(ano_admissao as integer) as ano_admissao
    from source
    where sequencial is not null
)

select * from renamed
