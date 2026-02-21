-- Grain: 1 row per senator currently in office
-- Source: data/raw/senadores.parquet (extracted from Senate Open Data API)
-- Responsibility: type casting and null-safe field selection only — no business logic here

select
    senador_id,
    nome_parlamentar,
    nome_completo,
    sexo,
    try_cast(data_nascimento as date)  as data_nascimento,
    foto_url,
    pagina_url,
    email,
    partido_sigla,
    estado_sigla,
    naturalidade,
    uf_naturalidade
from read_parquet('../data/raw/senadores.parquet')
where senador_id is not null
  and senador_id != ''
