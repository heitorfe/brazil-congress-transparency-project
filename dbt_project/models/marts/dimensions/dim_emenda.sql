-- Dimension: Parliamentary amendments
-- Grain: 1 row per unique codigo_emenda
-- Source: stg_transparencia__emendas_documentos (deduplicated via QUALIFY)
--
-- Senator linking:
--   Uses name normalization (accent removal, uppercase) to join nome_autor_emenda
--   against dim_senador.nome_parlamentar. Only "Emenda Individual" tipo_emenda
--   rows will match senators; bancada/committee/rapporteur amendments will not.
--   senador_id will be NULL for non-individual amendments and historical legislators
--   no longer in the Senate.

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
        numero_emenda
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

final as (
    select
        e.codigo_emenda,
        e.ano_emenda,
        e.tipo_emenda,
        e.codigo_autor_emenda,
        e.nome_autor_emenda,
        e.numero_emenda,

        -- Senator linkage (best-effort via normalized name match)
        s.senador_id,
        s.nome_parlamentar,
        s.partido_sigla,
        s.estado_sigla,

        -- Helper flag for dashboard filtering
        s.senador_id is not null                                    as is_senador_atual,
        e.tipo_emenda like '%Individual%'                           as is_emenda_individual

    from emendas_dedup e
    left join senators s
        on {{ normalize_name('e.nome_autor_emenda') }} = s.nome_norm
        and e.tipo_emenda like '%Individual%'
)

select * from final
