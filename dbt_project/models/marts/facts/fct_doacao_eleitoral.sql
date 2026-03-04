{{
  config(
    materialized='incremental',
    unique_key='donation_id',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns',
    tags=['facts', 'tse', 'donations']
  )
}}

-- Grain: 1 row per campaign donation receipt
-- Source: stg_tse__doacoes (all election years)
-- Join: sq_candidato + ano → dim_candidato for denormalized candidate attributes
-- Incremental watermark: load only new election years on subsequent runs

with doacoes as (
    select * from {{ ref('stg_tse__doacoes') }}

    {% if is_incremental() %}
    where ano >= (select coalesce(max(ano), 2017) from {{ this }})
    {% endif %}
),

candidatos as (
    select
        sq_candidato,
        ano_eleicao,
        nome_candidato,
        cargo,
        uf         as candidato_uf,
        partido_sigla,
        eleito,
        senador_id,
        deputado_id
    from {{ ref('dim_candidato') }}
)

select
    d.donation_id,
    d.sq_candidato,
    d.ano,
    c.nome_candidato,
    c.cargo,
    c.candidato_uf,
    c.partido_sigla,
    c.eleito,
    c.senador_id,
    c.deputado_id,
    d.uf                     as doacao_uf,
    d.cpf_cnpj_doador_raw,
    d.nome_doador,
    d.nome_doador_rfb,
    d.cnae_doador,
    d.cnae_descricao,
    d.tipo_doador,
    d.origem_receita,
    d.natureza_receita,
    d.especie_receita,
    d.valor_receita,
    d.valor_receita_raw,
    d.data_receita
from doacoes d
left join candidatos c
    on  c.sq_candidato = d.sq_candidato
    and c.ano_eleicao  = d.ano
