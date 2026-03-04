{{ config(materialized='view') }}

-- Grain: 1 row per campaign donation receipt
-- Source: data/raw/tse_doacoes_*.parquet  (all election years via glob)
-- Key: donation_id (stable SHA256[:16] built in extractor from sq_candidato|ano|cpf|valor|data)
-- BRL parsing: "1.234,56" → 1234.56 using REPLACE/REPLACE/TRY_CAST (same pattern as Phase 4A)

with source as (
    select * from read_parquet('../data/raw/tse_doacoes_*.parquet')
),

renamed as (
    select
        trim(donation_id)                                as donation_id,
        trim(sq_candidato)                               as sq_candidato,
        try_cast(ano as integer)                         as ano,
        trim(uf)                                         as uf,
        nullif(trim(cpf_cnpj_doador_raw), '')            as cpf_cnpj_doador_raw,
        nullif(trim(nome_doador), '')                    as nome_doador,
        nullif(trim(nome_doador_rfb), '')                as nome_doador_rfb,
        nullif(trim(cnae_doador), '')                    as cnae_doador,
        nullif(trim(cnae_descricao), '')                 as cnae_descricao,
        nullif(trim(partido_doador), '')                 as partido_doador,
        nullif(trim(origem_receita), '')                 as origem_receita,
        nullif(trim(natureza_receita), '')               as natureza_receita,
        nullif(trim(especie_receita), '')                as especie_receita,

        -- Parse BRL locale: "1.234,56" → 1234.56
        try_cast(
            replace(replace(trim(valor_receita_raw), '.', ''), ',', '.')
            as decimal(14, 2)
        )                                                as valor_receita,
        trim(valor_receita_raw)                          as valor_receita_raw,

        try_cast(
            nullif(trim(data_receita), '') as date
        )                                                as data_receita,

        -- Classify donor document type based on digit count
        case
            when length(regexp_replace(coalesce(trim(cpf_cnpj_doador_raw), ''), '[^0-9]', '', 'g')) = 14
                then 'CNPJ'
            when length(regexp_replace(coalesce(trim(cpf_cnpj_doador_raw), ''), '[^0-9]', '', 'g')) = 11
                then 'CPF'
            else 'unknown'
        end                                              as tipo_doador

    from source
    where donation_id is not null
      and trim(donation_id) != ''
      and sq_candidato is not null
      and trim(sq_candidato) != ''
)

select * from renamed
where valor_receita > 0
