{{
  config(
    materialized='table',
    tags=['dimensions']
  )
}}

-- Grain: 1 row per committee (active, across SF / CN / CD)
-- Source: stg_legis__comissoes
--   Merged from /comissao/lista/colegiados (primary) + /comissao/lista/mistas (CN augmentation)
-- New in Phase 2: finalidade, sigla_tipo, codigo_tipo, publica, member counts, fonte

with source as (
    select * from {{ ref('stg_legis__comissoes') }}
),

final as (
    select
        codigo_comissao,
        sigla_comissao,
        nome_comissao,
        finalidade,
        sigla_casa,
        codigo_tipo,
        sigla_tipo,
        descricao_tipo,
        data_inicio,
        data_fim,
        publica,
        -- member composition (only populated for joint CN committees via /lista/mistas)
        qtd_titulares,
        qtd_senadores_titulares,
        qtd_deputados_titulares,
        fonte
    from source
)

select * from final
