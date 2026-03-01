{{ config(materialized='view') }}

-- Grain: 1 row per expense reimbursement receipt
-- Source: data/raw/ceaps.parquet (ADM API /api/v1/senadores/despesas_ceaps/{ano})
-- Key quirk: cod_senador is an INTEGER in the ADM API — cast to VARCHAR for FK join to dim_senador

with source as (
    select * from read_parquet('../data/raw/ceaps.parquet')
),

renamed as (
    select
        cast(id as bigint)                         as id,
        -- ADM API returns cod_senador as integer; VARCHAR needed for FK join to dim_senador
        cast(cod_senador as varchar)               as senador_id,
        trim(nome_senador)                         as nome_senador,
        cast(ano as integer)                       as ano,
        cast(mes as integer)                       as mes,
        -- Abbreviate verbose uppercase ADM category names so all downstream charts
        -- render readable axis labels without Python-side string mapping.
        case trim(tipo_despesa)
            when 'LOCOMOÇÃO, HOSPEDAGEM, ALIMENTAÇÃO, COMBUSTÍVEIS E LUBRIFICANTES'
                then 'Locomoção / Hospedagem'
            when 'DIVULGAÇÃO DA ATIVIDADE PARLAMENTAR.'
                then 'Divulgação parlamentar'
            when 'PASSAGENS AÉREAS, AQUÁTICAS E TERRESTRES NACIONAIS'
                then 'Passagens aéreas'
            when 'SERVIÇOS DE SEGURANÇA PRIVADA'
                then 'Segurança privada'
            when 'CONSULTORIAS, PESQUISAS E TRABALHOS TÉCNICOS.'
                then 'Consultorias técnicas'
            when 'SERVIÇOS POSTAIS'
                then 'Serviços postais'
            when 'LOCAÇÃO DE IMÓVEIS PARA ESCRITÓRIO POLÍTICO, COMITÊ E AFINS.'
                then 'Aluguel de escritório'
            when 'LOCAÇÃO DE IMÓVEIS PARA ESCRITÓRIO POLÍTICO'
                then 'Aluguel de escritório'
            when 'ASSINATURAS DE PUBLICAÇÕES'
                then 'Assinaturas'
            when 'SERVIÇOS DE ADVOCACIA'
                then 'Advocacia'
            when 'SERVIÇOS GRÁFICOS'
                then 'Serviços gráficos'
            when 'SERVIÇOS MÉDICOS'
                then 'Serviços médicos'
            when 'OUTRAS DESPESAS COM PESSOAL DECORRENTES DE CONTRATO DE TERCEIRIZAÇÃO'
                then 'Terceirização de pessoal'
            when 'ALIMENTAÇÃO'
                then 'Alimentação'
            when 'COMBUSTÍVEIS E LUBRIFICANTES'
                then 'Combustíveis'
            when 'HOSPEDAGEM, EXCETO DO PARLAMENTAR'
                then 'Hospedagem'
            else lower(trim(tipo_despesa))
        end                                        as tipo_despesa,
        trim(cnpj_cpf)                             as cnpj_cpf,
        trim(fornecedor)                           as fornecedor,
        trim(documento)                            as documento,
        try_cast(data as date)                     as data,
        trim(detalhamento)                         as detalhamento,
        cast(valor_reembolsado as decimal(12, 2))  as valor_reembolsado,
        trim(tipo_documento)                       as tipo_documento
    from source
    where id is not null
      and cod_senador is not null
)

select * from renamed
