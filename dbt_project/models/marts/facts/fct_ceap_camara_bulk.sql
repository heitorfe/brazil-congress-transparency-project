{{
  config(
    materialized='incremental',
    unique_key='expense_id',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns',
    tags=['facts', 'expenses', 'camara']
  )
}}

-- Grain: 1 row per Chamber CEAP expense reimbursement receipt (2009–present)
-- Source: stg_camara__ceap_bulk (bulk ZIP/CSV from camara.leg.br)
-- Replaces fct_despesa_deputado (REST API, current legislature only) as dashboard target.
-- Join: deputado_id (bigint) → dim_deputado.deputado_id

with ceap as (
    select * from {{ ref('stg_camara__ceap_bulk') }}

    {% if is_incremental() %}
    -- On incremental runs, load from the last loaded year onward
    where ano >= (select coalesce(max(ano), 2008) from {{ this }})
    {% endif %}
),

deputados as (
    select
        deputado_id,
        nome_parlamentar,
        sigla_partido,
        sigla_uf
    from {{ ref('dim_deputado') }}
),

final as (
    select
        c.expense_id,
        c.deputado_id,
        d.nome_parlamentar             as nome_parlamentar_dim,
        d.sigla_partido                as partido_sigla_dim,
        d.sigla_uf                     as estado_sigla_dim,
        c.nome_parlamentar,
        c.cpf,
        c.uf,
        c.partido_sigla,
        c.cod_legislatura,
        c.ano,
        c.mes,
        c.num_sub_cota,
        c.tipo_despesa,
        c.descricao_especificacao,
        c.fornecedor,
        c.cnpj_cpf,
        c.tipo_fornecedor,
        c.numero_documento,
        c.data,
        c.valor_documento,
        c.valor_glosa,
        c.valor_liquido
    from ceap c
    left join deputados d using (deputado_id)
)

select * from final
