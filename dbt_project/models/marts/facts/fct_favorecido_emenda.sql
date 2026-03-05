{{
  config(
    materialized='incremental',
    unique_key='favorecido_id',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns',
    tags=['facts', 'transferegov']
  )
}}

-- Grain: 1 row per (emenda × recipient) — all fiscal years
-- Source: stg_transferegov__favorecidos (consolidated snapshot, ~800K rows)
-- Join: codigo_emenda → dim_emenda for legislator/party denormalization
-- Incremental watermark: ano

with favorecidos as (
    select * from {{ ref('stg_transferegov__favorecidos') }}

    {% if is_incremental() %}
    where ano >= (select coalesce(max(ano), 2014) from {{ this }})
    {% endif %}
),

emendas as (
    select
        codigo_emenda,
        tipo_emenda,
        nome_autor_emenda,
        codigo_autor_emenda,
        numero_emenda,
        senador_id,
        deputado_id,
        nome_parlamentar_senador,
        nome_parlamentar_deputado,
        partido_sigla_senador,
        partido_sigla_deputado,
        estado_sigla_senador,
        estado_sigla_deputado,
        is_senador_atual,
        is_deputado_atual
    from {{ ref('dim_emenda') }}
)

select
    f.favorecido_id,
    f.codigo_emenda,
    f.ano,
    f.codigo_favorecido,
    f.nome_favorecido,
    f.tipo_pessoa,
    f.tipo_doc_favorecido,
    f.natureza_juridica,
    f.municipio_favorecido,
    f.uf_favorecido,
    cast(f.valor_transferido as double) as valor_transferido,
    cast(f.valor_empenhado   as double) as valor_empenhado,
    cast(f.valor_pago        as double) as valor_pago,
    e.tipo_emenda,
    e.nome_autor_emenda,
    e.codigo_autor_emenda,
    e.numero_emenda,
    e.senador_id,
    e.deputado_id,
    e.nome_parlamentar_senador,
    e.nome_parlamentar_deputado,
    e.partido_sigla_senador,
    e.partido_sigla_deputado,
    e.estado_sigla_senador,
    e.estado_sigla_deputado,
    e.is_senador_atual,
    e.is_deputado_atual
from favorecidos f
left join emendas e using (codigo_emenda)
