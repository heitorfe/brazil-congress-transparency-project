{{ config(materialized='view') }}

-- Grain: 1 row per candidate per election year
-- Source: data/raw/tse_candidatos_*.parquet  (all election years via glob)
-- Key: sq_candidato — unique within an election year; a candidate who runs in
--      multiple elections gets a different sq_candidato each time.
-- CPF: NULL when TSE masked it (sentinel "-4" → set to NULL in extractor).

with source as (
    select * from read_parquet('../data/raw/tse_candidatos_*.parquet')
),

renamed as (
    select
        trim(sq_candidato)                               as sq_candidato,
        try_cast(ano_eleicao as integer)                 as ano_eleicao,
        trim(nome_candidato)                             as nome_candidato,
        upper(trim(nome_candidato))                      as nome_candidato_norm,
        trim(nome_urna)                                  as nome_urna,
        upper(trim(nome_urna))                           as nome_urna_norm,
        trim(cargo)                                      as cargo,
        trim(uf)                                         as uf,
        trim(municipio)                                  as municipio,
        trim(partido_sigla)                              as partido_sigla,
        trim(nr_candidato)                               as nr_candidato,
        trim(situacao_turno)                             as situacao_turno,
        -- Eleito flag: ELEITO (1st or 2nd round) and ELEITO POR MÉDIA
        upper(trim(situacao_turno)) like 'ELEITO%'       as eleito,
        trim(genero)                                     as genero,
        trim(grau_instrucao)                             as grau_instrucao,
        trim(ocupacao)                                   as ocupacao,
        try_cast(nullif(trim(idade_posse), '') as integer) as idade_posse,
        -- cpf_raw: NULL when TSE masked it; kept as-is otherwise for future enrichment
        nullif(trim(cpf_raw), '')                        as cpf_raw

    from source
    where sq_candidato is not null
      and trim(sq_candidato) != ''
)

select * from renamed
