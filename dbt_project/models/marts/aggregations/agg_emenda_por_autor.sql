-- Aggregation: Parliamentary amendments summarized by author × year × amendment type
-- Grain: 1 row per (nome_autor_emenda × ano_emenda × tipo_emenda)
--
-- Pre-aggregated from fct_emenda_documento to avoid full table scans at dashboard time.
-- fct_emenda_documento can have millions of rows across all years — this table
-- reduces it to a few thousand rows for KPI cards, rankings, and trend charts.
--
-- Payment phase filter: only "Pagamento" (actual disbursements), not "Empenho"/"Liquidação"
-- (commitments / accruals), to avoid triple-counting the same transaction.

with payments as (
    select *
    from {{ ref('fct_emenda_documento') }}
    where fase_despesa = 'Pagamento'
),

aggregated as (
    select
        nome_autor_emenda,
        codigo_autor_emenda,
        ano_emenda,
        tipo_emenda,

        -- Senator linkage (will be NULL for bancada/committee/rapporteur amendments)
        max(senador_id)                             as senador_id,
        max(nome_parlamentar)                       as nome_parlamentar,
        max(partido_sigla)                          as partido_sigla,
        max(estado_sigla)                           as estado_sigla,
        bool_or(is_senador_atual)                   as is_senador_atual,

        -- Volume metrics
        count(*)                                    as num_documentos,
        count(distinct codigo_emenda)               as num_emendas,
        count(distinct codigo_favorecido)           as num_favorecidos_distintos,
        count(distinct municipio_recurso)           as num_municipios_distintos,
        count(distinct uf_recurso)                  as num_ufs_distintos,

        -- Financial totals (paid amounts only — Pagamento phase)
        sum(coalesce(valor_pago, 0))                as total_pago,
        sum(coalesce(valor_empenhado, 0))           as total_empenhado,
        avg(coalesce(valor_pago, 0))                as media_pago_por_documento

    from payments
    group by
        nome_autor_emenda,
        codigo_autor_emenda,
        ano_emenda,
        tipo_emenda
)

select * from aggregated
order by ano_emenda desc, total_pago desc
