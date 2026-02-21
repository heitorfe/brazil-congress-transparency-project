{{
  config(
    materialized='table',
    tags=['dimensions']
  )
}}

-- Grain: 1 row per senator × committee × role period
-- Source: stg_legis__membros_comissao (historical from /senador/{code}/comissoes)
-- Surrogate key hashes (senador_id, codigo_comissao, descricao_participacao, data_inicio)

with source as (
    select * from {{ ref('stg_legis__membros_comissao') }}
),

final as (
    select
        md5(
            coalesce(cast(senador_id as varchar), '') || '|' ||
            coalesce(cast(codigo_comissao as varchar), '') || '|' ||
            coalesce(cast(descricao_participacao as varchar), '') || '|' ||
            coalesce(cast(data_inicio as varchar), '')
        )                              as membro_sk,
        senador_id,
        codigo_comissao,
        sigla_comissao,
        nome_comissao,
        sigla_casa,
        descricao_participacao,
        data_inicio,
        data_fim,
        data_fim is null               as is_current
    from source
)

select * from final
