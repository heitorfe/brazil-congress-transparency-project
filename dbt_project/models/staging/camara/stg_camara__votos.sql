{{ config(materialized='view') }}

-- Grain: 1 row per deputy × voting session
-- Source: data/raw/camara_votos.parquet
-- tipo_voto values: "Sim", "Não", "Abstenção", "Obstrução", "Artigo 17", etc.

with source as (
    select * from read_parquet('../data/raw/camara_votos.parquet')
),

renamed as (
    select
        trim(votacao_id)                            as votacao_id,
        cast(deputado_id as varchar)                as deputado_id,
        trim(nome)                                  as nome,
        upper(trim(sigla_partido))                  as sigla_partido,
        upper(trim(sigla_uf))                       as sigla_uf,
        cast(id_legislatura as integer)             as id_legislatura,
        trim(tipo_voto)                             as tipo_voto,
        try_cast(data_registro as timestamp)        as data_registro,
        -- Derived boolean flags (mirrors Senate stg_legis__votos pattern)
        case when trim(tipo_voto) = 'Sim'       then true else false end  as voto_afirmativo,
        case when trim(tipo_voto) = 'Não'       then true else false end  as voto_negativo,
        case when trim(tipo_voto) in ('Abstenção', 'Obstrução', 'Artigo 17')
             then true else false end                                      as presente_sem_voto
    from source
    where votacao_id is not null
      and votacao_id != ''
      and deputado_id is not null
      and deputado_id != ''
)

select * from renamed
