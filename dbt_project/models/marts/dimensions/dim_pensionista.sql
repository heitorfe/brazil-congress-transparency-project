{{
  config(
    materialized='table',
    tags=['dimensions']
  )
}}

-- Grain: 1 row per Senate pensioner (current snapshot)
-- PK: sequencial (ADM API integer ID, stable across payroll records)
-- This is a full-refresh dimension — the API returns current state only.
-- data_obito and data_inicio_pensao are cast to DATE in staging.

with source as (
    select * from {{ ref('stg_adm__pensionistas') }}
)

select
    sequencial,
    nome,
    vinculo,
    fundamento,
    cargo_nome,
    funcao_nome,
    categoria_codigo,
    categoria_nome,
    nome_instituidor,
    ano_exercicio,
    data_obito,
    data_inicio_pensao
from source
order by nome
