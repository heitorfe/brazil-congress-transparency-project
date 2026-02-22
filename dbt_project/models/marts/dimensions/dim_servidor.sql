{{
  config(
    materialized='table',
    tags=['dimensions']
  )
}}

-- Grain: 1 row per Senate staff member (current snapshot)
-- PK: sequencial (ADM API integer ID, stable across payroll records)
-- This is a full-refresh dimension — the API returns current state only.
-- Payroll facts join here via sequencial to get lotacao, vinculo, situacao.

with source as (
    select * from {{ ref('stg_adm__servidores') }}
)

select
    sequencial,
    nome,
    vinculo,
    situacao,
    cargo_nome,
    padrao,
    especialidade,
    funcao_nome,
    lotacao_sigla,
    lotacao_nome,
    categoria_codigo,
    categoria_nome,
    cedido_tipo,
    cedido_orgao_origem,
    cedido_orgao_destino,
    ano_admissao
from source
order by nome
