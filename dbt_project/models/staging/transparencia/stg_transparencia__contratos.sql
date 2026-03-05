{{ config(materialized='view', tags=['transparencia', 'contratos']) }}

-- Grain: 1 row per federal procurement contract
-- Source: read_parquet('../data/raw/transparencia_contratos_*.parquet') — one file per YYYYMM
-- Responsibilities:
--   • BRL locale parsing (1.234,56 → 1234.56)
--   • Date parsing DD/MM/YYYY → date
--   • Supplier type classification (CNPJ=14d / CPF=11d)
--   • Filter nulls and zero-value contracts

with source as (
    select * from read_parquet('../data/raw/transparencia_contratos_*.parquet')
    where contrato_id is not null
),

renamed as (
    select
        contrato_id,
        yyyymm,
        trim(codigo_orgao_superior)  as codigo_orgao_superior,
        trim(nome_orgao_superior)    as nome_orgao_superior,
        trim(codigo_orgao)           as codigo_orgao,
        trim(nome_orgao)             as nome_orgao,
        trim(codigo_ug)              as codigo_ug,
        trim(nome_ug)                as nome_ug,
        trim(modalidade_compra)      as modalidade_compra,
        trim(situacao_contrato)      as situacao_contrato,
        trim(numero_contrato)        as numero_contrato,
        trim(cnpj_contratado)        as cnpj_contratado,
        trim(nome_contratado)        as nome_contratado,
        upper(trim(nome_contratado)) as nome_contratado_norm,
        trim(objeto)                 as objeto,
        -- BRL value parsing (format: 1.234,56)
        try_cast(
            replace(replace(trim(valor_inicial_raw), '.', ''), ',', '.')
            as double
        )                            as valor_inicial,
        valor_inicial_raw,
        try_cast(
            replace(replace(trim(valor_final_raw), '.', ''), ',', '.')
            as double
        )                            as valor_final,
        valor_final_raw,
        -- Date parsing DD/MM/YYYY
        try_strptime(nullif(trim(data_assinatura_raw),      ''), '%d/%m/%Y') as data_assinatura,
        try_strptime(nullif(trim(data_inicio_vigencia_raw), ''), '%d/%m/%Y') as data_inicio_vigencia,
        try_strptime(nullif(trim(data_fim_vigencia_raw),    ''), '%d/%m/%Y') as data_fim_vigencia,
        -- ano_contrato derived from signature date; fallback to ano_compra field
        coalesce(
            try_cast(nullif(trim(ano_compra), '') as integer),
            year(try_strptime(nullif(trim(data_assinatura_raw), ''), '%d/%m/%Y'))
        )                            as ano_contrato,
        -- Supplier type by CNPJ/CPF digit count
        case
            when length(regexp_replace(coalesce(trim(cnpj_contratado), ''), '[^0-9]', '', 'g')) = 14
                then 'CNPJ'
            when length(regexp_replace(coalesce(trim(cnpj_contratado), ''), '[^0-9]', '', 'g')) = 11
                then 'CPF'
            else 'unknown'
        end                          as tipo_contratado,
        trim(numero_licitacao)       as numero_licitacao
    from source
)

-- Dedup: same contract can appear in multiple monthly ZIPs (open contracts span months).
-- Keep the earliest yyyymm occurrence so contrato_id is unique downstream.
select * from renamed
where valor_inicial > 0
  and valor_inicial is not null
qualify row_number() over (partition by contrato_id order by yyyymm) = 1
