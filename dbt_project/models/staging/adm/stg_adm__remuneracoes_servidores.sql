{{ config(materialized='view') }}

-- Grain: 1 row per (sequencial × ano × mes × tipo_folha)
-- Source: data/raw/remuneracoes_servidores.parquet
--   ADM API /api/v1/servidores/remuneracoes/{ano}/{mes}, 2019-present
-- The ADM API returns all monetary amounts as STRING (not numeric).
-- This model casts every monetary field to decimal(12, 2).
-- tipo_folha distinguishes regular vs supplementary payrolls in the same month.

with source as (
    select * from read_parquet('../data/raw/remuneracoes_servidores.parquet')
),

renamed as (
    select
        cast(sequencial as bigint)                         as sequencial,
        trim(nome)                                         as nome,
        cast(ano as integer)                               as ano,
        cast(mes as integer)                               as mes,
        trim(tipo_folha)                                   as tipo_folha,

        -- Gross components (positive contributions to pay)
        -- ADM API returns monetary values in Brazilian locale ("36.380,05") — strip '.' then swap ',' → '.'
        try_cast(replace(replace(remuneracao_basica::varchar,        '.', ''), ',', '.') as decimal(12, 2)) as remuneracao_basica,
        try_cast(replace(replace(vantagens_pessoais::varchar,        '.', ''), ',', '.') as decimal(12, 2)) as vantagens_pessoais,
        try_cast(replace(replace(funcao_comissionada::varchar,       '.', ''), ',', '.') as decimal(12, 2)) as funcao_comissionada,
        try_cast(replace(replace(gratificacao_natalina::varchar,     '.', ''), ',', '.') as decimal(12, 2)) as gratificacao_natalina,
        try_cast(replace(replace(horas_extras::varchar,              '.', ''), ',', '.') as decimal(12, 2)) as horas_extras,
        try_cast(replace(replace(outras_eventuais::varchar,          '.', ''), ',', '.') as decimal(12, 2)) as outras_eventuais,
        try_cast(replace(replace(diarias::varchar,                   '.', ''), ',', '.') as decimal(12, 2)) as diarias,
        try_cast(replace(replace(auxilios::varchar,                  '.', ''), ',', '.') as decimal(12, 2)) as auxilios,
        try_cast(replace(replace(abono_permanencia::varchar,         '.', ''), ',', '.') as decimal(12, 2)) as abono_permanencia,
        try_cast(replace(replace(vantagens_indenizatorias::varchar,  '.', ''), ',', '.') as decimal(12, 2)) as vantagens_indenizatorias,

        -- Deductions (these reduce take-home pay)
        try_cast(replace(replace(faltas::varchar,                    '.', ''), ',', '.') as decimal(12, 2)) as faltas,
        try_cast(replace(replace(previdencia::varchar,               '.', ''), ',', '.') as decimal(12, 2)) as previdencia,
        try_cast(replace(replace(reversao_teto_constitucional::varchar, '.', ''), ',', '.') as decimal(12, 2)) as reversao_teto_constitucional,
        try_cast(replace(replace(imposto_renda::varchar,             '.', ''), ',', '.') as decimal(12, 2)) as imposto_renda,

        -- Net take-home (after all deductions)
        try_cast(replace(replace(remuneracao_liquida::varchar,       '.', ''), ',', '.') as decimal(12, 2)) as remuneracao_liquida,

        -- Time-series key — makes incremental merge conditions cleaner
        make_date(cast(ano as integer), cast(mes as integer), 1) as data_competencia

    from source
    where sequencial is not null
      and ano is not null
      and mes is not null
)

select * from renamed
