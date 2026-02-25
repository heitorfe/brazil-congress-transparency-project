-- Staging: Parliamentary amendments aggregated summary
-- Source: Portal da Transparência — emendas-parlamentares single unified file (all years)
-- Grain: 1 row per (codigo_emenda × codigo_acao × localidade)
--
-- Responsibilities:
--   • Type casting from raw String Parquet
--   • Parse BR decimal format for all 6 monetary fields
--   • Filter out rows without an amendment code

with source as (
    select * from read_parquet('../data/raw/emendas_parlamentares.parquet')
    where codigo_emenda is not null
      and codigo_emenda != ''
      and codigo_emenda != 'Sem informação'
),

final as (
    select
        -- Identity
        codigo_emenda,
        try_cast(ano_emenda as integer)                                                             as ano_emenda,
        codigo_autor_emenda,
        nome_autor_emenda,
        numero_emenda,
        tipo_emenda,

        -- Location
        localidade_recurso,
        municipio,
        codigo_municipio_ibge,
        uf,
        codigo_uf_ibge,
        regiao,

        -- Budget classification
        codigo_funcao,
        nome_funcao,
        codigo_subfuncao,
        nome_subfuncao,
        codigo_programa,
        nome_programa,
        codigo_acao,
        nome_acao,
        codigo_plano_orcamentario,
        nome_plano_orcamentario,

        -- Monetary amounts (BR format: "1.234.567,89" → 1234567.89)
        try_cast(replace(replace(valor_empenhado,        '.', ''), ',', '.') as decimal(14, 2))     as valor_empenhado,
        try_cast(replace(replace(valor_liquidado,        '.', ''), ',', '.') as decimal(14, 2))     as valor_liquidado,
        try_cast(replace(replace(valor_pago,             '.', ''), ',', '.') as decimal(14, 2))     as valor_pago,
        try_cast(replace(replace(valor_restos_inscrito,  '.', ''), ',', '.') as decimal(14, 2))     as valor_restos_inscrito,
        try_cast(replace(replace(valor_restos_cancelado, '.', ''), ',', '.') as decimal(14, 2))     as valor_restos_cancelado,
        try_cast(replace(replace(valor_restos_pagos,     '.', ''), ',', '.') as decimal(14, 2))     as valor_restos_pagos

    from source
)

select * from final
