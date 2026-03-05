{{
  config(
    materialized='incremental',
    unique_key='contrato_id',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns',
    tags=['facts', 'transparencia', 'contratos']
  )
}}

-- Grain: 1 row per federal procurement contract
-- Source: stg_transparencia__contratos (glob all monthly Parquets)
-- Incremental watermark: ano_contrato

with contratos as (
    select * from {{ ref('stg_transparencia__contratos') }}

    {% if is_incremental() %}
    where ano_contrato >= (select coalesce(max(ano_contrato), 2022) from {{ this }})
    {% endif %}
)

select * from contratos
