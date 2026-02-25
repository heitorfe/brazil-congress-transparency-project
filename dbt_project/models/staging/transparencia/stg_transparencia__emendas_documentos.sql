-- Staging: Parliamentary amendments by SIAFI expense document
-- Source: Portal da Transparência — emendas-parlamentares-documentos yearly ZIPs (2014–current)
-- Grain: 1 row per (codigo_emenda × codigo_documento × fase_despesa)
--
-- Responsibilities:
--   • Type casting (all columns arrive as String from Parquet)
--   • Parse BR decimal format for monetary fields: "1.234,56" → 1234.56
--   • Parse DD/MM/YYYY date strings
--   • Filter out rows without an amendment code

with source as (
    select * from read_parquet('../data/raw/emendas_documentos.parquet')
    where codigo_emenda is not null
      and codigo_emenda != ''
      and codigo_emenda != 'Sem informação'
),

final as (
    select
        -- Identity
        codigo_emenda,
        try_cast(ano_emenda as integer)                                                     as ano_emenda,
        codigo_autor_emenda,
        nome_autor_emenda,
        numero_emenda,
        tipo_emenda,

        -- Document
        codigo_documento,
        try_cast(strptime(data_documento, '%d/%m/%Y') as date)                             as data_documento,
        fase_despesa,

        -- Location where resources were applied
        localidade_recurso,
        uf_recurso,
        municipio_recurso,
        codigo_ibge_municipio,

        -- Beneficiary (favorecido)
        codigo_favorecido,
        favorecido,
        tipo_favorecido,
        uf_favorecido,
        municipio_favorecido,

        -- Budget execution units
        codigo_ug,
        ug,
        codigo_unidade_orcamentaria,
        unidade_orcamentaria,
        codigo_orgao,
        orgao,
        codigo_orgao_superior,
        orgao_superior,

        -- Budget classification
        codigo_grupo_despesa,
        grupo_despesa,
        codigo_elemento_despesa,
        elemento_despesa,
        codigo_modalidade_aplicacao,
        modalidade_aplicacao,
        codigo_plano_orcamentario,
        plano_orcamentario,
        codigo_funcao,
        funcao,
        codigo_subfuncao,
        subfuncao,
        codigo_programa,
        programa,
        codigo_acao,
        acao,
        linguagem_cidada,
        codigo_subtitulo,
        subtitulo,

        -- Flags
        case
            when upper(trim(coalesce(possui_convenio, ''))) in ('SIM', 'S', 'TRUE', '1') then true
            when upper(trim(coalesce(possui_convenio, ''))) in ('NAO', 'NÃO', 'N', 'FALSE', '0') then false
            else null
        end                                                                                 as possui_convenio,

        -- Monetary amounts (BR format: "1.234,56" → 1234.56)
        try_cast(replace(replace(valor_empenhado, '.', ''), ',', '.') as decimal(14, 2))   as valor_empenhado,
        try_cast(replace(replace(valor_pago,      '.', ''), ',', '.') as decimal(14, 2))   as valor_pago

    from source
)

select * from final
