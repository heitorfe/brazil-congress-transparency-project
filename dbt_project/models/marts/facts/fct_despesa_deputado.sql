{{
  config(
    materialized='incremental',
    unique_key=['cod_documento', 'deputado_id'],
    incremental_strategy='merge',
    on_schema_change='sync_all_columns',
    tags=['facts', 'expenses']
  )
}}

-- Grain: 1 row per CEAP expense document for Chamber deputies
-- Source: stg_camara__despesas (Chamber API /deputados/{id}/despesas)
-- Mirrors fct_ceaps (Senate CEAPS) but uses native float values — no decimal parsing needed.

with despesas as (
    select * from {{ ref('stg_camara__despesas') }}

    {% if is_incremental() %}
    where ano > (
        select coalesce(max(ano), 2018)
        from {{ this }}
    )
    {% endif %}
),

deputados as (
    select deputado_id, nome_parlamentar, sigla_uf, sigla_partido
    from {{ ref('dim_deputado') }}
),

final as (
    select
        d.cod_documento,
        d.deputado_id,
        dep.nome_parlamentar                as nome_parlamentar_dim,
        dep.sigla_uf                        as sigla_uf_dim,
        dep.sigla_partido                   as sigla_partido_dim,
        d.ano,
        d.mes,
        d.tipo_despesa,
        d.tipo_documento,
        d.data_documento,
        d.num_documento,
        d.nome_fornecedor,
        d.cnpj_cpf_fornecedor,
        d.valor_documento,
        d.valor_liquido,
        d.valor_glosa,
        d.num_ressarcimento,
        d.cod_lote,
        d.parcela,
        d.url_documento
    from despesas d
    left join deputados dep using (deputado_id)
)

select * from final
