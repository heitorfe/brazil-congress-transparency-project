-- Grain: 1 row per senator currently in office
-- Joins biographical data with their most recent active mandate
-- partido_nome comes from the TSE seed (not the API, which doesn't return it)

with senadores as (
    select * from {{ ref('stg_senadores') }}
),

mandatos_ranked as (
    -- Rank mandates per senator; take only the most recent one
    select
        *,
        row_number() over (
            partition by senador_id
            order by mandato_inicio desc nulls last
        ) as rn
    from {{ ref('stg_mandatos') }}
),

mandato_atual as (
    select * from mandatos_ranked where rn = 1
),

partidos as (
    select partido_sigla, partido_nome, partido_numero_tse
    from {{ ref('dim_partido') }}
),

final as (
    select
        s.senador_id,
        s.nome_parlamentar,
        s.nome_completo,
        s.sexo,
        s.data_nascimento,
        s.foto_url,
        s.pagina_url,
        s.email,
        s.naturalidade,
        s.uf_naturalidade,
        s.partido_sigla,
        p.partido_nome,
        p.partido_numero_tse,
        -- Prefer the state from the mandate (more reliable) over the list field
        coalesce(m.estado_sigla, s.estado_sigla)  as estado_sigla,
        m.mandato_id,
        m.mandato_inicio,
        m.mandato_fim,
        m.descricao_participacao,
        m.legislatura_inicio,
        m.legislatura_fim,
        -- Senator is in active exercise when mandate end is null or in the future
        case
            when m.mandato_fim is null then true
            when m.mandato_fim >= current_date then true
            else false
        end                                        as em_exercicio
    from senadores s
    left join mandato_atual m using (senador_id)
    left join partidos p using (partido_sigla)
)

select * from final
