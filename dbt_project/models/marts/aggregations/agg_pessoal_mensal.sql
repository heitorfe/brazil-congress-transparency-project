{{
  config(
    materialized='table',
    tags=['aggregations']
  )
}}

-- Grain: 1 row per (ano × mes × vinculo × lotacao_sigla)
-- Pre-aggregates fct_remuneracao_servidor to avoid scanning ~840k rows at dashboard render time.
-- vinculo and lotacao_sigla are the two most common grouping dimensions in the dashboard.
-- NULL lotacao_sigla is preserved (staff records with no current lotacao assignment).

with remuneracoes as (
    select * from {{ ref('fct_remuneracao_servidor') }}
),

aggregated as (
    select
        ano,
        mes,
        data_competencia,
        coalesce(vinculo, 'NÃO INFORMADO')      as vinculo,
        coalesce(lotacao_sigla, 'NÃO INFORMADO') as lotacao_sigla,
        coalesce(lotacao_nome, 'Não informado')  as lotacao_nome,

        count(distinct sequencial)               as num_servidores,
        sum(remuneracao_liquida)                 as total_liquido,
        sum(remuneracao_bruta)                   as total_bruto,
        sum(horas_extras)                        as total_horas_extras_valor,
        sum(remuneracao_basica)                  as total_basica,
        sum(funcao_comissionada)                 as total_comissionada,
        sum(vantagens_pessoais)                  as total_vantagens_pessoais,
        sum(vantagens_indenizatorias)            as total_indenizatorias
    from remuneracoes
    group by 1, 2, 3, 4, 5, 6
)

select * from aggregated
order by ano, mes, vinculo, lotacao_sigla
