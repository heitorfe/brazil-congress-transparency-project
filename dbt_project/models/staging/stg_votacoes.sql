-- Grain: 1 row per voting session (one bill × one plenary vote event)
-- Source: data/raw/votacoes.parquet (extracted from Senate Open Data API /votacao)
-- Responsibility: type casting and field renaming only — no business logic

with source as (
    select * from read_parquet('../data/raw/votacoes.parquet')
),

renamed as (
    select
        cast(codigo_sessao_votacao    as bigint)  as codigo_sessao_votacao,
        cast(codigo_votacao_sve       as bigint)  as codigo_votacao_sve,
        cast(codigo_sessao            as bigint)  as codigo_sessao,
        cast(codigo_sessao_legislativa as bigint) as codigo_sessao_legislativa,
        upper(trim(sigla_tipo_sessao))            as sigla_tipo_sessao,
        cast(numero_sessao            as integer) as numero_sessao,
        -- dataSessao arrives as ISO datetime string "2024-02-20T00:00:00"
        try_cast(left(data_sessao, 10) as date)   as data_sessao,
        cast(id_processo              as bigint)  as id_processo,
        cast(codigo_materia           as bigint)  as codigo_materia,
        trim(identificacao)                       as identificacao,
        upper(trim(sigla_materia))                as sigla_materia,
        trim(numero_materia)                      as numero_materia,
        cast(ano_materia              as integer) as ano_materia,
        try_cast(left(data_apresentacao, 10) as date) as data_apresentacao,
        trim(ementa)                              as ementa,
        cast(sequencial_sessao        as integer) as sequencial_sessao,
        upper(trim(votacao_secreta))              as votacao_secreta,
        trim(descricao_votacao)                   as descricao_votacao,
        upper(trim(resultado_votacao))            as resultado_votacao,
        cast(total_votos_sim          as integer) as total_votos_sim,
        cast(total_votos_nao          as integer) as total_votos_nao,
        cast(total_votos_abstencao    as integer) as total_votos_abstencao,
        trim(informe_texto)                       as informe_texto
    from source
    where codigo_sessao_votacao is not null
)

select * from renamed
