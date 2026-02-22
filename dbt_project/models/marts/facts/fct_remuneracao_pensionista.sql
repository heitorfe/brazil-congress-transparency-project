{{
  config(
    materialized='incremental',
    unique_key=['sequencial', 'ano', 'mes'],
    incremental_strategy='merge',
    on_schema_change='sync_all_columns',
    tags=['facts']
  )
}}

-- Grain: 1 row per (sequencial × ano × mes)
-- Pensioner payroll has fewer gross components than staff payroll
-- (no horas_extras, diarias, auxilios, faltas — pensioners don't work).
-- Enriched with dim_pensionista for vinculo and nome_instituidor.

with remuneracoes as (
    select * from {{ ref('stg_adm__remuneracoes_pensionistas') }}
    {% if is_incremental() %}
    where data_competencia > (
        select coalesce(max(data_competencia), '2019-01-01'::date)
        from {{ this }}
    )
    {% endif %}
),

pensionistas as (
    select
        sequencial,
        vinculo,
        categoria_nome,
        cargo_nome,
        nome_instituidor,
        data_inicio_pensao
    from {{ ref('dim_pensionista') }}
),

final as (
    select
        r.sequencial,
        r.nome,
        r.ano,
        r.mes,
        r.tipo_folha,
        r.data_competencia,

        -- Dimension attributes (denormalized)
        p.vinculo,
        p.categoria_nome,
        p.cargo_nome,
        p.nome_instituidor,
        p.data_inicio_pensao,

        -- Gross pay components
        r.remuneracao_basica,
        r.vantagens_pessoais,
        r.funcao_comissionada,
        r.gratificacao_natalina,
        r.vantagens_indenizatorias,

        -- Deductions
        r.previdencia,
        r.reversao_teto_constitucional,
        r.imposto_renda,

        -- Net
        r.remuneracao_liquida,

        -- Computed gross
        coalesce(r.remuneracao_basica, 0)
            + coalesce(r.vantagens_pessoais, 0)
            + coalesce(r.funcao_comissionada, 0)
            + coalesce(r.gratificacao_natalina, 0)
            + coalesce(r.vantagens_indenizatorias, 0)
            as remuneracao_bruta

    from remuneracoes r
    left join pensionistas p using (sequencial)
)

select * from final
