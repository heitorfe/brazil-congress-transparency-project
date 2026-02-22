{{ config(materialized='view') }}

-- Grain: 1 row per (sequencial × ano_pagamento × mes_pagamento)
-- Source: data/raw/horas_extras.parquet
--   ADM API /api/v1/servidores/horas-extras/{ano}/{mes}, 2019-present
-- The nested per-day detail (horas_extras[] array) is intentionally NOT exploded;
-- we store only the monthly summary (valor_total) which is sufficient for trend analysis.
-- mes_ano_prestacao/mes_ano_pagamento are raw display strings from the API;
-- ano_pagamento and mes_pagamento are integer-parsed from the URL parameters (authoritative).

with source as (
    select * from read_parquet('../data/raw/horas_extras.parquet')
),

renamed as (
    select
        cast(sequencial as bigint)                                as sequencial,
        trim(nome)                                                as nome,
        -- ADM API returns monetary values in Brazilian locale ("5.013,00") — strip '.' then swap ',' → '.'
        try_cast(replace(replace(valor_total::varchar, '.', ''), ',', '.') as decimal(12, 2)) as valor_total,
        trim(mes_ano_prestacao)                                   as mes_ano_prestacao,
        trim(mes_ano_pagamento)                                   as mes_ano_pagamento,
        cast(ano_pagamento as integer)                            as ano_pagamento,
        cast(mes_pagamento as integer)                            as mes_pagamento,
        make_date(cast(ano_pagamento as integer), cast(mes_pagamento as integer), 1) as data_competencia
    from source
    where sequencial is not null
      and ano_pagamento is not null
      and mes_pagamento is not null
)

select * from renamed
