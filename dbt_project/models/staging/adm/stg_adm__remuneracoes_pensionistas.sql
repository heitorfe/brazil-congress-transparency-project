{{ config(materialized='view') }}

-- Grain: 1 row per (sequencial × ano × mes)
-- Source: data/raw/remuneracoes_pensionistas.parquet
--   ADM API /api/v1/servidores/pensionistas/remuneracoes/{ano}/{mes}, 2019-present
-- Pensioner payroll has fewer components than staff payroll:
--   missing horas_extras, diarias, auxilios, faltas (pensioners don't work).

with source as (
    select * from read_parquet('../data/raw/remuneracoes_pensionistas.parquet')
),

renamed as (
    select
        cast(sequencial as bigint)                         as sequencial,
        trim(nome)                                         as nome,
        cast(ano as integer)                               as ano,
        cast(mes as integer)                               as mes,
        trim(tipo_folha)                                   as tipo_folha,

        -- Gross components
        -- ADM API returns monetary values in Brazilian locale ("36.380,05") — strip '.' then swap ',' → '.'
        try_cast(replace(replace(remuneracao_basica::varchar,           '.', ''), ',', '.') as decimal(12, 2)) as remuneracao_basica,
        try_cast(replace(replace(vantagens_pessoais::varchar,           '.', ''), ',', '.') as decimal(12, 2)) as vantagens_pessoais,
        try_cast(replace(replace(funcao_comissionada::varchar,          '.', ''), ',', '.') as decimal(12, 2)) as funcao_comissionada,
        try_cast(replace(replace(gratificacao_natalina::varchar,        '.', ''), ',', '.') as decimal(12, 2)) as gratificacao_natalina,
        try_cast(replace(replace(vantagens_indenizatorias::varchar,     '.', ''), ',', '.') as decimal(12, 2)) as vantagens_indenizatorias,

        -- Deductions
        try_cast(replace(replace(previdencia::varchar,                  '.', ''), ',', '.') as decimal(12, 2)) as previdencia,
        try_cast(replace(replace(reversao_teto_constitucional::varchar, '.', ''), ',', '.') as decimal(12, 2)) as reversao_teto_constitucional,
        try_cast(replace(replace(imposto_renda::varchar,                '.', ''), ',', '.') as decimal(12, 2)) as imposto_renda,

        -- Net
        try_cast(replace(replace(remuneracao_liquida::varchar,          '.', ''), ',', '.') as decimal(12, 2)) as remuneracao_liquida,

        make_date(cast(ano as integer), cast(mes as integer), 1) as data_competencia

    from source
    where sequencial is not null
      and ano is not null
      and mes is not null
)

select * from renamed
