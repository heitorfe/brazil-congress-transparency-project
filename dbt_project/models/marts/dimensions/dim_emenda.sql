-- Dimension: Parliamentary amendments
-- Grain: 1 row per unique codigo_emenda
-- Source: stg_transparencia__emendas_documentos (deduplicated via QUALIFY)
--
-- Author linking strategy (senators take priority over deputies):
--   1. Normalize nome_autor_emenda (accent removal, uppercase) once in a CTE.
--   2. Left-join against dim_senador on normalized name for "Emenda Individual" rows.
--   3. Left-join against dim_deputado on normalized name ONLY when no senator matched.
--   This avoids false ties between senators and deputies who share a name.
--   senador_id / deputado_id will both be NULL for non-individual amendments
--   and for historical legislators no longer in either chamber.

with emendas_raw as (
    select * from {{ ref('stg_transparencia__emendas_documentos') }}
),

emendas_dedup as (
    -- Take the most recent document per amendment to get representative metadata
    select
        codigo_emenda,
        ano_emenda,
        tipo_emenda,
        codigo_autor_emenda,
        nome_autor_emenda,
        numero_emenda,
        -- Pre-compute the normalized name here to avoid repeating the macro
        {{ normalize_name('nome_autor_emenda') }} as nome_autor_norm
    from emendas_raw
    qualify row_number() over (
        partition by codigo_emenda
        order by data_documento desc nulls last, codigo_documento desc
    ) = 1
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

deputados as (
    select
        deputado_id,
        nome_parlamentar,
        sigla_partido  as partido_sigla,
        sigla_uf       as estado_sigla,
        {{ normalize_name('nome_parlamentar') }} as nome_norm
    from {{ ref('dim_deputado') }}
),

final as (
    select
        e.codigo_emenda,
        e.ano_emenda,
        e.tipo_emenda,
        e.codigo_autor_emenda,
        e.nome_autor_emenda,
        e.numero_emenda,

        -- ── Senator linkage (best-effort via normalized name) ──────────────
        s.senador_id,
        s.nome_parlamentar                             as nome_parlamentar_senador,
        s.partido_sigla                                as partido_sigla_senador,
        s.estado_sigla                                 as estado_sigla_senador,

        -- ── Deputy linkage (only tried when no senator matched) ────────────
        -- The join condition `s.senador_id is null` prevents matching a deputy
        -- when a senator was already found for this amendment.
        d.deputado_id,
        d.nome_parlamentar                             as nome_parlamentar_deputado,
        d.partido_sigla                                as partido_sigla_deputado,
        d.estado_sigla                                 as estado_sigla_deputado,

        -- ── Convenience flags ──────────────────────────────────────────────
        s.senador_id is not null                       as is_senador_atual,
        d.deputado_id is not null                      as is_deputado_atual,
        e.tipo_emenda like '%Individual%'              as is_emenda_individual

    from emendas_dedup e

    -- Step 1: try senator match
    left join senators s
        on e.nome_autor_norm = s.nome_norm
        and e.tipo_emenda like '%Individual%'

    -- Step 2: try deputy match only when senator was not found
    left join deputados d
        on s.senador_id is null
        and e.nome_autor_norm = d.nome_norm
        and e.tipo_emenda like '%Individual%'

    -- Guard against name collisions (two senators or two deputies with same
    -- normalized name both matching the same emenda). Senator priority, then
    -- deputy, then arbitrary deterministic tiebreak.
    qualify row_number() over (
        partition by e.codigo_emenda
        order by
            s.senador_id nulls last,
            d.deputado_id nulls last
    ) = 1
)

select * from final
