{{ config(materialized='view') }}

-- Grain: 1 row per senator per voting session
-- Source: data/raw/votos.parquet (exploded from nested votos[] in /votacao API)
-- Responsibility: type casting, renaming, and derived boolean flags

with source as (
    select * from read_parquet('../data/raw/votos.parquet')
),

renamed as (
    select
        cast(codigo_sessao_votacao as bigint)  as codigo_sessao_votacao,
        cast(codigo_parlamentar    as bigint)  as codigo_parlamentar,
        -- Cast to string to join with dim_senador.senador_id (stored as string)
        cast(codigo_parlamentar    as varchar) as senador_id,
        trim(nome_parlamentar)                 as nome_parlamentar,
        upper(trim(sexo_parlamentar))          as sexo_parlamentar,
        upper(trim(sigla_partido))             as sigla_partido,
        upper(trim(sigla_uf))                  as sigla_uf,
        trim(sigla_voto)                       as sigla_voto,
        trim(descricao_voto)                   as descricao_voto,
        -- Derived: did this senator cast an affirmative vote?
        case when trim(sigla_voto) = 'Sim' then true else false end  as voto_afirmativo,
        -- Derived: did this senator cast a negative vote?
        case when trim(sigla_voto) = 'Não' then true else false end  as voto_negativo,
        -- Derived: was the senator absent (not voting, regardless of reason)?
        case
            when trim(sigla_voto) in ('AP', 'LS', 'MIS') then true
            else false
        end as ausente,
        -- Derived: was the senator present but did not register a vote?
        case
            when trim(sigla_voto) in ('P-NRV', 'Abstenção') then true
            else false
        end as presente_sem_voto
    from source
    where codigo_sessao_votacao is not null
      and codigo_parlamentar    is not null
)

select * from renamed
