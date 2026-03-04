{{ config(tags=['aggregations', 'tse']) }}

-- Grain: 1 row per (sq_candidato, ano)
-- Pre-aggregated for dashboard performance — fct_doacao_eleitoral has 800K+ rows
-- per election year. The dashboard queries this table directly.

select
    sq_candidato,
    ano,
    nome_candidato,
    cargo,
    candidato_uf,
    partido_sigla,
    eleito,
    senador_id,
    deputado_id,
    cast(sum(valor_receita) as double)                                           as total_arrecadado,
    count(*)                                                                     as num_doacoes,
    count(distinct cpf_cnpj_doador_raw)                                          as num_doadores,
    count(distinct cnae_doador)                                                  as num_cnaes_distintos,
    cast(sum(case when tipo_doador = 'CNPJ' then valor_receita else 0 end) as double)
                                                                                 as total_pessoa_juridica,
    cast(sum(case when tipo_doador = 'CPF'  then valor_receita else 0 end) as double)
                                                                                 as total_pessoa_fisica
from {{ ref('fct_doacao_eleitoral') }}
group by 1, 2, 3, 4, 5, 6, 7, 8, 9
