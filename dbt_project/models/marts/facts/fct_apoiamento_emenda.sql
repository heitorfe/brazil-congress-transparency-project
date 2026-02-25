-- Fact: Parliamentary amendment co-sponsor (apoiamento) records
-- Grain: 1 row per (empenho × codigo_apoiador)
-- Source: stg_transparencia__apoiamento_emendas + dim_senador (for supporter)
--
-- Both the amendment author AND the co-sponsor (apoiador) are linked to
-- dim_senador via name normalization. Either or both may be NULL if the
-- legislator is not a current senator.

with apoiamentos as (
    select * from {{ ref('stg_transparencia__apoiamento_emendas') }}
),

senators as (
    select
        senador_id,
        nome_parlamentar,
        partido_sigla,
        estado_sigla,
        {{ normalize_name('nome_parlamentar') }} as nome_norm
    from {{ ref('dim_senador') }}
),

final as (
    select
        -- Commitment identity
        a.empenho,
        a.ano_emenda,
        a.codigo_emenda,
        a.tipo_emenda,

        -- Amendment author
        a.codigo_autor_emenda,
        a.nome_autor_emenda,

        -- Supporter (apoiador) — the legislator who backed this commitment
        a.codigo_apoiador,
        a.nome_apoiador,
        a.data_apoio,
        a.data_retirada_apoio,
        a.data_ultima_movimentacao_empenho,

        -- Supporter senator linkage (nullable)
        s_apoiador.senador_id                   as senador_id_apoiador,
        s_apoiador.nome_parlamentar             as nome_parlamentar_apoiador,
        s_apoiador.partido_sigla                as partido_sigla_apoiador,
        s_apoiador.estado_sigla                 as estado_sigla_apoiador,

        -- Beneficiary
        a.codigo_favorecido,
        a.favorecido,
        a.tipo_favorecido,
        a.uf_favorecido,
        a.municipio_favorecido,

        -- Budget
        a.codigo_orgao,
        a.orgao,
        a.codigo_acao,
        a.acao,

        -- Monetary
        a.valor_empenhado,
        a.valor_cancelado,
        a.valor_pago

    from apoiamentos a
    left join senators s_apoiador
        on {{ normalize_name('a.nome_apoiador') }} = s_apoiador.nome_norm
)

select * from final
