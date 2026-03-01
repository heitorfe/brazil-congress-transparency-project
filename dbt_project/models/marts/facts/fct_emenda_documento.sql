-- Fact: Parliamentary amendment expense documents
-- Grain: 1 row per (codigo_emenda × codigo_documento × fase_despesa)
-- Source: stg_transparencia__emendas_documentos + dim_emenda
--
-- Configuration: Incremental merge — new years of data are added without
-- rescanning the entire history. Watermark: ano_emenda (integer year).

{{
    config(
        materialized='incremental',
        unique_key=['codigo_emenda', 'codigo_documento', 'fase_despesa'],
        incremental_strategy='merge',
        on_schema_change='sync_all_columns',
    )
}}

with documents as (
    select * from {{ ref('stg_transparencia__emendas_documentos') }}
    {% if is_incremental() %}
    where ano_emenda >= (
        select coalesce(max(ano_emenda), 2014)
        from {{ this }}
    )
    {% endif %}
),

dim as (
    select
        codigo_emenda,
        senador_id,
        nome_parlamentar_senador,
        partido_sigla_senador,
        estado_sigla_senador,
        deputado_id,
        nome_parlamentar_deputado,
        partido_sigla_deputado,
        estado_sigla_deputado,
        is_senador_atual,
        is_deputado_atual,
        is_emenda_individual
    from {{ ref('dim_emenda') }}
),

final as (
    select
        -- Keys
        d.codigo_emenda,
        d.codigo_documento,
        d.fase_despesa,
        d.ano_emenda,
        d.data_documento,

        -- Amendment metadata (denormalized from dim_emenda)
        d.tipo_emenda,
        d.codigo_autor_emenda,
        d.nome_autor_emenda,
        d.numero_emenda,

        -- Author linkage (senator or deputy, nullable for non-individual amendments)
        e.senador_id,
        e.nome_parlamentar_senador,
        e.partido_sigla_senador,
        e.estado_sigla_senador,
        e.deputado_id,
        e.nome_parlamentar_deputado,
        e.partido_sigla_deputado,
        e.estado_sigla_deputado,
        e.is_senador_atual,
        e.is_deputado_atual,
        e.is_emenda_individual,

        -- Location
        d.localidade_recurso,
        d.uf_recurso,
        d.municipio_recurso,
        d.codigo_ibge_municipio,

        -- Beneficiary
        d.codigo_favorecido,
        d.favorecido,
        d.tipo_favorecido,
        d.uf_favorecido,
        d.municipio_favorecido,

        -- Budget execution
        d.codigo_ug,
        d.ug,
        d.codigo_orgao,
        d.orgao,
        d.codigo_orgao_superior,
        d.orgao_superior,

        -- Budget classification
        d.codigo_grupo_despesa,
        d.grupo_despesa,
        d.codigo_elemento_despesa,
        d.elemento_despesa,
        d.codigo_acao,
        d.acao,
        d.linguagem_cidada,
        d.codigo_funcao,
        d.funcao,
        d.codigo_programa,
        d.programa,
        d.possui_convenio,

        -- Monetary amounts
        d.valor_empenhado,
        d.valor_pago

    from documents d
    left join dim e using (codigo_emenda)
)

select * from final
