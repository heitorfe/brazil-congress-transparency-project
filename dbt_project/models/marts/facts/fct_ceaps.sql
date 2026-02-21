{{
  config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns',
    tags=['facts', 'expenses']
  )
}}

-- Grain: 1 row per CEAPS expense reimbursement receipt
-- Source: stg_adm__ceaps (ADM API /api/v1/senadores/despesas_ceaps/{ano} from 2019)
-- FK: senador_id (cast from integer cod_senador in staging) → dim_senador.senador_id

with ceaps as (
    select * from {{ ref('stg_adm__ceaps') }}

    {% if is_incremental() %}
    -- On incremental runs, load only new IDs not yet in the warehouse
    where id > (
        select coalesce(max(id), 0)
        from {{ this }}
    )
    {% endif %}
),

senadores as (
    select senador_id, nome_parlamentar, estado_sigla
    from {{ ref('dim_senador') }}
),

final as (
    select
        c.id,
        c.senador_id,
        c.nome_senador,
        s.nome_parlamentar                as nome_parlamentar_dim,
        s.estado_sigla                    as estado_sigla_dim,
        c.ano,
        c.mes,
        c.tipo_despesa,
        c.cnpj_cpf,
        c.fornecedor,
        c.documento,
        c.data,
        c.detalhamento,
        c.valor_reembolsado,
        c.tipo_documento
    from ceaps c
    left join senadores s using (senador_id)
)

select * from final
