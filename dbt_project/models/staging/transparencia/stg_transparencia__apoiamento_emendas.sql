-- Staging: Parliamentary amendment co-sponsor (apoiamento) records
-- Source: Portal da Transparência — apoiamento-emendas-parlamentares yearly ZIPs (2020–2025)
-- Grain: 1 row per (empenho × codigo_apoiador)
--
-- Responsibilities:
--   • Type casting from raw String Parquet
--   • Parse BR decimal format for monetary fields
--   • Parse DD/MM/YYYY date strings
--   • Filter out rows without an empenho identifier

with source as (
    select * from read_parquet('../data/raw/apoiamento_emendas.parquet')
    where empenho is not null
      and empenho != ''
),

final as (
    select
        -- Supporter (apoiador = the legislator backing this commitment)
        codigo_apoiador,
        nome_apoiador,
        -- Dates may appear as DD/MM/YYYY or ISO 8601 depending on the year of data
        case
            when data_apoio like '%/%/%'
            then try_cast(strptime(data_apoio, '%d/%m/%Y') as date)
            else try_cast(data_apoio as date)
        end                                                                                 as data_apoio,
        case
            when data_retirada_apoio like '%/%/%'
            then try_cast(strptime(data_retirada_apoio, '%d/%m/%Y') as date)
            else try_cast(data_retirada_apoio as date)
        end                                                                                 as data_retirada_apoio,
        try_cast(data_ultima_movimentacao_empenho as timestamp)                            as data_ultima_movimentacao_empenho,

        -- Commitment (empenho)
        empenho,

        -- Beneficiary
        codigo_favorecido,
        favorecido,
        tipo_favorecido,
        uf_favorecido,
        municipio_favorecido,

        -- Amendment identity
        codigo_emenda,
        codigo_autor_emenda,
        nome_autor_emenda,
        numero_emenda,
        try_cast(ano_emenda as integer)                                                     as ano_emenda,
        tipo_emenda,
        localidade_recurso,

        -- Budget execution units
        codigo_ug,
        ug,
        codigo_unidade_orcamentaria,
        unidade_orcamentaria,
        codigo_orgao,
        orgao,
        codigo_orgao_superior,
        orgao_superior,
        codigo_acao,
        acao,

        -- Monetary amounts
        try_cast(replace(replace(valor_empenhado, '.', ''), ',', '.') as decimal(14, 2))   as valor_empenhado,
        try_cast(replace(replace(valor_cancelado, '.', ''), ',', '.') as decimal(14, 2))   as valor_cancelado,
        try_cast(replace(replace(valor_pago,      '.', ''), ',', '.') as decimal(14, 2))   as valor_pago

    from source
)

select * from final
