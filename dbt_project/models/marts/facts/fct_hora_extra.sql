{{
  config(
    materialized='table',
    tags=['facts']
  )
}}

-- Grain: 1 row per (sequencial × ano_pagamento × mes_pagamento)
-- Full refresh (not incremental) — overtime data is small (~50k rows total)
-- and the ADM API may backfill earlier months, making full refresh safer.
-- Enriched with dim_servidor for lotacao_sigla and vinculo.

with horas as (
    select * from {{ ref('stg_adm__horas_extras') }}
),

servidores as (
    select
        sequencial,
        lotacao_sigla,
        lotacao_nome,
        vinculo,
        cargo_nome
    from {{ ref('dim_servidor') }}
),

final as (
    select
        h.sequencial,
        h.nome,
        h.ano_pagamento,
        h.mes_pagamento,
        h.data_competencia,
        h.valor_total,
        h.mes_ano_prestacao,
        h.mes_ano_pagamento,

        -- Dimension attributes (denormalized)
        s.lotacao_sigla,
        s.lotacao_nome,
        s.vinculo,
        s.cargo_nome

    from horas h
    left join servidores s using (sequencial)
)

select * from final
