{{
  config(
    materialized='incremental',
    unique_key=['sequencial', 'ano', 'mes', 'tipo_folha'],
    incremental_strategy='merge',
    on_schema_change='sync_all_columns',
    tags=['facts']
  )
}}

-- Grain: 1 row per (sequencial × ano × mes × tipo_folha)
-- tipo_folha distinguishes regular payroll from supplementary (13th salary, etc.)
-- Incremental merge: only loads months newer than the latest data_competencia in the table.
-- Enriched with dim_servidor for lotacao_sigla, vinculo, situacao — denormalized
-- for query convenience (avoids joining at dashboard time).
-- remuneracao_bruta is computed here (business logic stays in the warehouse, not Python).

with remuneracoes as (
    select * from {{ ref('stg_adm__remuneracoes_servidores') }}
    {% if is_incremental() %}
    where data_competencia > (
        select coalesce(max(data_competencia), '2019-01-01'::date)
        from {{ this }}
    )
    {% endif %}
),

servidores as (
    select
        sequencial,
        lotacao_sigla,
        lotacao_nome,
        vinculo,
        situacao,
        cargo_nome,
        categoria_nome
    from {{ ref('dim_servidor') }}
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
        s.lotacao_sigla,
        s.lotacao_nome,
        s.vinculo,
        s.situacao,
        s.cargo_nome,
        s.categoria_nome,

        -- Gross pay components
        r.remuneracao_basica,
        r.vantagens_pessoais,
        r.funcao_comissionada,
        r.gratificacao_natalina,
        r.horas_extras,
        r.outras_eventuais,
        r.diarias,
        r.auxilios,
        r.abono_permanencia,
        r.vantagens_indenizatorias,

        -- Deductions
        r.faltas,
        r.previdencia,
        r.reversao_teto_constitucional,
        r.imposto_renda,

        -- Net
        r.remuneracao_liquida,

        -- Computed gross total (all positive pay components summed)
        coalesce(r.remuneracao_basica, 0)
            + coalesce(r.vantagens_pessoais, 0)
            + coalesce(r.funcao_comissionada, 0)
            + coalesce(r.gratificacao_natalina, 0)
            + coalesce(r.horas_extras, 0)
            + coalesce(r.outras_eventuais, 0)
            + coalesce(r.diarias, 0)
            + coalesce(r.auxilios, 0)
            + coalesce(r.abono_permanencia, 0)
            + coalesce(r.vantagens_indenizatorias, 0)
            as remuneracao_bruta

    from remuneracoes r
    left join servidores s using (sequencial)
)

select * from final
