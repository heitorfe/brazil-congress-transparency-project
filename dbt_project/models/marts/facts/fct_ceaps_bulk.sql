{{
  config(
    materialized='incremental',
    unique_key='expense_id',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns',
    tags=['facts', 'expenses', 'senate']
  )
}}

-- Grain: 1 row per Senate CEAPS expense reimbursement receipt (2008–present)
-- Source: stg_legis__ceaps_bulk (bulk CSV from senado.leg.br)
-- Replaces fct_ceaps (ADM API, 2019-only) as the dashboard query target.
-- Join: senador_nome (normalized) → dim_senador.nome_parlamentar

with ceaps as (
    select * from {{ ref('stg_legis__ceaps_bulk') }}

    {% if is_incremental() %}
    -- On incremental runs, load from the last loaded year onward
    where ano >= (select coalesce(max(ano), 2007) from {{ this }})
    {% endif %}
),

senadores as (
    select
        senador_id,
        upper(trim(nome_parlamentar)) as nome_parlamentar_norm,
        nome_parlamentar,
        partido_sigla,
        estado_sigla
    from {{ ref('dim_senador') }}
),

final as (
    select
        c.expense_id,
        s.senador_id,
        c.senador_nome,
        s.nome_parlamentar             as nome_parlamentar_dim,
        s.partido_sigla                as partido_sigla_dim,
        s.estado_sigla                 as estado_sigla_dim,
        c.ano,
        c.mes,
        c.tipo_despesa,
        c.cnpj_cpf,
        c.tipo_documento,
        c.fornecedor,
        c.documento,
        c.data,
        c.detalhamento,
        c.valor_reembolsado
    from ceaps c
    left join senadores s
        on upper(trim(c.senador_nome)) = s.nome_parlamentar_norm
)

select * from final
