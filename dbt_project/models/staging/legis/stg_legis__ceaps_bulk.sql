{{ config(materialized='view') }}

-- Grain: 1 row per Senate CEAPS expense reimbursement receipt
-- Source: data/raw/ceaps_senado_*.parquet (bulk CSV 2008–present)
-- Key: expense_id (SHA256[:16] composite, generated in extractor)
-- Join: senador_nome → dim_senador.nome_parlamentar (name-based, no numeric ID in bulk CSV)

with source as (
    select * from read_parquet('../data/raw/ceaps_senado_*.parquet')
),

renamed as (
    select
        expense_id,
        cast(ano as integer)                                as ano,
        nullif(trim(mes), '')                               as mes_str,
        try_cast(nullif(trim(mes), '') as integer)          as mes,
        upper(trim(senador_nome))                           as senador_nome,

        -- Abbreviate verbose category names (mirrors stg_adm__ceaps mapping)
        case upper(trim(tipo_despesa))
            when upper('Aluguel de imóveis para escritório político, compreendendo despesas concernentes a eles.')
                then 'Aluguel de escritório'
            when upper('LOCOMOÇÃO, HOSPEDAGEM, ALIMENTAÇÃO, COMBUSTÍVEIS E LUBRIFICANTES')
                then 'Locomoção / Hospedagem'
            when upper('DIVULGAÇÃO DA ATIVIDADE PARLAMENTAR.')
                then 'Divulgação parlamentar'
            when upper('PASSAGENS AÉREAS, AQUÁTICAS E TERRESTRES NACIONAIS')
                then 'Passagens aéreas'
            when upper('SERVIÇOS DE SEGURANÇA PRIVADA')
                then 'Segurança privada'
            when upper('CONSULTORIAS, PESQUISAS E TRABALHOS TÉCNICOS.')
                then 'Consultorias técnicas'
            when upper('SERVIÇOS POSTAIS')
                then 'Serviços postais'
            when upper('LOCAÇÃO DE IMÓVEIS PARA ESCRITÓRIO POLÍTICO, COMITÊ E AFINS.')
                then 'Aluguel de escritório'
            when upper('LOCAÇÃO DE IMÓVEIS PARA ESCRITÓRIO POLÍTICO')
                then 'Aluguel de escritório'
            when upper('ASSINATURAS DE PUBLICAÇÕES')
                then 'Assinaturas'
            when upper('SERVIÇOS DE ADVOCACIA')
                then 'Advocacia'
            when upper('SERVIÇOS GRÁFICOS')
                then 'Serviços gráficos'
            when upper('SERVIÇOS MÉDICOS')
                then 'Serviços médicos'
            when upper('OUTRAS DESPESAS COM PESSOAL DECORRENTES DE CONTRATO DE TERCEIRIZAÇÃO')
                then 'Terceirização de pessoal'
            when upper('ALIMENTAÇÃO')
                then 'Alimentação'
            when upper('COMBUSTÍVEIS E LUBRIFICANTES')
                then 'Combustíveis'
            when upper('HOSPEDAGEM, EXCETO DO PARLAMENTAR')
                then 'Hospedagem'
            else lower(trim(tipo_despesa))
        end                                                 as tipo_despesa,

        trim(cnpj_cpf)                                      as cnpj_cpf,
        trim(tipo_documento)                                as tipo_documento,
        trim(fornecedor)                                    as fornecedor,
        trim(documento)                                     as documento,
        try_cast(nullif(trim(data), '') as date)            as data,
        trim(detalhamento)                                  as detalhamento,

        -- Parse BRL locale string: "1.234,56" → 1234.56
        try_cast(
            replace(replace(valor_reembolsado, '.', ''), ',', '.')
            as decimal(12, 2)
        )                                                   as valor_reembolsado

    from source
    where expense_id is not null
      and senador_nome is not null
      and senador_nome != ''
)

select * from renamed
