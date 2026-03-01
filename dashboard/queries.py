"""
Thin query layer — all DuckDB interactions live here.
Dashboard pages import from this module; they never touch DuckDB directly.
"""

import duckdb
import polars as pl
from pathlib import Path

# Absolute path — robust regardless of working directory when Streamlit launches
DB_PATH = Path(__file__).parent.parent / "data" / "warehouse" / "senate.duckdb"


def _con() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(DB_PATH), read_only=True)

def list_tables() -> list[str]:
    """List all tables in the main_marts schema."""
    with _con() as con:
        rows = con.execute("SHOW TABLES FROM main_marts").fetchall()
    return [r[0] for r in rows]

def get_all_senators() -> pl.DataFrame:
    """All senators currently in office."""
    with _con() as con:
        return con.execute("""
            SELECT
                senador_id,
                nome_parlamentar,
                nome_completo,
                sexo,
                data_nascimento,
                foto_url,
                pagina_url,
                email,
                partido_sigla,
                partido_nome,
                estado_sigla,
                mandato_inicio,
                mandato_fim,
                descricao_participacao,
                em_exercicio
            FROM main_marts.dim_senador
            WHERE em_exercicio = true
            ORDER BY nome_parlamentar
        """).pl()


def get_senator_by_id(senador_id: str) -> pl.DataFrame:
    """Single senator row by ID."""
    with _con() as con:
        return con.execute("""
            SELECT *
            FROM main_marts.dim_senador
            WHERE senador_id = ?
        """, [senador_id]).pl()


def get_parties() -> list[str]:
    """Sorted list of party siglas with at least one senator in office."""
    with _con() as con:
        rows = con.execute("""
            SELECT partido_sigla FROM main_marts.dim_partido ORDER BY partido_sigla
        """).fetchall()
    return [r[0] for r in rows]

def adhoc_query(sql: str) -> pl.DataFrame:
    """Run any SQL query against the warehouse. Not intended for production use."""
    with _con() as con:
        return con.execute(sql).pl()


# ── National aggregates ────────────────────────────────────────────────────

def get_party_composition() -> pl.DataFrame:
    """Senator count per party (active senators only)."""
    with _con() as con:
        return con.execute("""
            SELECT
                partido_sigla,
                partido_nome,
                COUNT(*) AS num_senadores
            FROM main_marts.dim_senador
            WHERE em_exercicio = true
            GROUP BY partido_sigla, partido_nome
            ORDER BY num_senadores DESC
        """).pl()


def get_mandate_classes() -> pl.DataFrame:
    """Senator count grouped by mandate-end year (for reelection class analysis)."""
    with _con() as con:
        return con.execute("""
            SELECT
                EXTRACT(year FROM mandato_fim)::INTEGER AS ano_fim,
                COUNT(*) AS num_senadores
            FROM main_marts.dim_senador
            WHERE em_exercicio = true
            GROUP BY ano_fim
            ORDER BY ano_fim
        """).pl()


# ── Committee queries ──────────────────────────────────────────────────────

def get_comissoes() -> pl.DataFrame:
    """All committees with member counts from the aggregation model."""
    with _con() as con:
        return con.execute("""
            SELECT
                c.codigo_comissao,
                c.sigla_comissao,
                c.nome_comissao,
                c.sigla_casa,
                c.descricao_tipo,
                c.data_inicio,
                c.data_fim,
                c.publica,
                COALESCE(a.num_membros_atuais, 0) AS num_membros_atuais,
                COALESCE(a.num_titulares, 0)      AS num_titulares,
                COALESCE(a.num_suplentes, 0)      AS num_suplentes
            FROM main_marts.dim_comissao c
            LEFT JOIN main_marts.agg_comissao_membros a
                ON c.codigo_comissao = a.codigo_comissao
            WHERE c.data_fim IS NULL
            ORDER BY c.sigla_casa, c.sigla_comissao
        """).pl()


def get_senator_comissoes(senador_id: str) -> pl.DataFrame:
    """Current committee memberships for a given senator."""
    with _con() as con:
        return con.execute("""
            SELECT
                sigla_comissao,
                nome_comissao,
                sigla_casa,
                descricao_participacao,
                data_inicio,
                data_fim,
                is_current
            FROM main_marts.dim_membro_comissao
            WHERE senador_id = ?
            ORDER BY is_current DESC, data_inicio DESC
        """, [senador_id]).pl()


def get_comissao_membros(codigo_comissao: str) -> pl.DataFrame:
    """Current members of a given committee, enriched with senator info."""
    with _con() as con:
        return con.execute("""
            SELECT
                m.senador_id,
                m.sigla_comissao,
                m.descricao_participacao AS cargo,
                m.data_inicio,
                s.nome_parlamentar,
                s.partido_sigla,
                s.estado_sigla
            FROM main_marts.dim_membro_comissao m
            LEFT JOIN main_marts.dim_senador s ON m.senador_id = s.senador_id
            WHERE m.codigo_comissao = ?
              AND m.is_current = true
            ORDER BY m.descricao_participacao, s.nome_parlamentar
        """, [codigo_comissao]).pl()


# ── Expense (CEAPS) queries ────────────────────────────────────────────────

def get_ceaps_summary_by_year() -> pl.DataFrame:
    """Total CEAPS spending per year across all senators."""
    with _con() as con:
        return con.execute("""
            SELECT
                ano,
                SUM(total_reembolsado)  AS total_gasto,
                COUNT(DISTINCT senador_id) AS num_senadores,
                SUM(qtd_recibos)        AS num_recibos
            FROM main_marts.agg_senador_despesas
            GROUP BY ano
            ORDER BY ano
        """).pl()


def get_ceaps_top_spenders(n: int = 15) -> pl.DataFrame:
    """Top N senators by total CEAPS reimbursement (all years)."""
    with _con() as con:
        return con.execute("""
            SELECT
                a.senador_id,
                COALESCE(s.nome_parlamentar, a.nome_senador) AS nome_parlamentar,
                s.partido_sigla,
                s.estado_sigla,
                SUM(a.total_reembolsado) AS total_gasto,
                SUM(a.qtd_recibos)       AS num_recibos
            FROM main_marts.agg_senador_despesas a
            LEFT JOIN main_marts.dim_senador s ON a.senador_id = s.senador_id
            GROUP BY a.senador_id, nome_parlamentar,a.nome_senador, s.partido_sigla, s.estado_sigla
            ORDER BY total_gasto DESC
            LIMIT ?
        """, [n]).pl()


def get_ceaps_by_year_and_category(ano: int) -> pl.DataFrame:
    """CEAPS breakdown by expense category for a given year."""
    with _con() as con:
        return con.execute("""
            SELECT
                tipo_despesa,
                SUM(total_reembolsado) AS total_gasto,
                SUM(qtd_recibos)       AS num_recibos
            FROM main_marts.agg_senador_despesas
            WHERE ano = ?
            GROUP BY tipo_despesa
            ORDER BY total_gasto DESC
        """, [ano]).pl()


def get_senator_ceaps(senador_id: str) -> pl.DataFrame:
    """Pre-aggregated CEAPS spending for a senator, by year + month + category."""
    with _con() as con:
        return con.execute("""
            SELECT
                ano,
                mes,
                tipo_despesa,
                qtd_recibos,
                total_reembolsado
            FROM main_marts.agg_senador_despesas
            WHERE senador_id = ?
            ORDER BY ano, mes
        """, [senador_id]).pl()


# ── Voting queries ─────────────────────────────────────────────────────────

def get_recent_voting_sessions(n: int = 100) -> pl.DataFrame:
    """
    Deduplicated plenary voting sessions (one row per session, not per senator).
    Uses MAX aggregation to get session-level data.
    """
    with _con() as con:
        return con.execute("""
            SELECT
                codigo_sessao_votacao,
                MAX(data_sessao)            AS data_sessao,
                MAX(sigla_materia)          AS sigla_materia,
                MAX(numero_materia)         AS numero_materia,
                MAX(ano_materia)            AS ano_materia,
                MAX(materia_identificacao)  AS materia_identificacao,
                MAX(materia_ementa)         AS materia_ementa,
                MAX(resultado_votacao)      AS resultado_votacao,
                MAX(sigla_tipo_sessao)      AS sigla_tipo_sessao,
                MAX(total_votos_sim)        AS total_votos_sim,
                MAX(total_votos_nao)        AS total_votos_nao,
                MAX(total_votos_abstencao)  AS total_votos_abstencao,
                COUNT(*)                    AS total_participantes
            FROM main_marts.fct_votacao
            GROUP BY codigo_sessao_votacao
            ORDER BY MAX(data_sessao) DESC
            LIMIT ?
        """, [n]).pl()


def get_senator_votes(senador_id: str, n: int = 200) -> pl.DataFrame:
    """Individual vote records for a specific senator."""
    with _con() as con:
        return con.execute("""
            SELECT
                data_sessao,
                materia_identificacao,
                materia_ementa,
                sigla_materia,
                numero_materia,
                ano_materia,
                sigla_voto,
                descricao_voto,
                voto_afirmativo,
                voto_negativo,
                ausente,
                presente_sem_voto,
                resultado_votacao,
                partido_sigla_voto
            FROM main_marts.fct_votacao
            WHERE senador_id = ?
            ORDER BY data_sessao DESC
            LIMIT ?
        """, [senador_id, n]).pl()


def get_senator_vote_summary(senador_id: str) -> pl.DataFrame:
    """Aggregate vote-type counts for a senator (for the accountability scorecard)."""
    with _con() as con:
        return con.execute("""
            SELECT
                COUNT(*)                                            AS total_votacoes,
                SUM(CASE WHEN voto_afirmativo  THEN 1 ELSE 0 END)  AS total_sim,
                SUM(CASE WHEN voto_negativo    THEN 1 ELSE 0 END)  AS total_nao,
                SUM(CASE WHEN presente_sem_voto THEN 1 ELSE 0 END) AS total_abstencao,
                SUM(CASE WHEN ausente          THEN 1 ELSE 0 END)  AS total_ausente,
                ROUND(
                    100.0 * SUM(CASE WHEN NOT ausente THEN 1 ELSE 0 END)
                    / NULLIF(COUNT(*), 0), 1
                ) AS taxa_presenca
            FROM main_marts.fct_votacao
            WHERE senador_id = ?
        """, [senador_id]).pl()


# ── Leadership queries ─────────────────────────────────────────────────────

def get_senator_liderancas(senador_id: str) -> pl.DataFrame:
    """Leadership positions held by a senator."""
    with _con() as con:
        return con.execute("""
            SELECT
                descricao_tipo_unidade,
                sigla_tipo_lideranca,
                descricao_tipo_lideranca,
                sigla_partido,
                nome_partido,
                data_designacao,
                casa
            FROM main_marts.dim_lideranca
            WHERE senador_id = ?
            ORDER BY data_designacao DESC
        """, [senador_id]).pl()


# ── Housing allowance ──────────────────────────────────────────────────────

def get_ceaps_top_categories(n: int = 12) -> pl.DataFrame:
    """Top N expense categories summed across all years — no year filter needed."""
    with _con() as con:
        return con.execute("""
            SELECT
                tipo_despesa,
                SUM(total_reembolsado) AS total_gasto,
                SUM(qtd_recibos)       AS num_recibos
            FROM main_marts.agg_senador_despesas
            GROUP BY tipo_despesa
            ORDER BY total_gasto DESC
            LIMIT ?
        """, [n]).pl()


def get_ceaps_categories_by_year() -> pl.DataFrame:
    """All categories × years for trend/stacked charts."""
    with _con() as con:
        return con.execute("""
            SELECT
                ano,
                tipo_despesa,
                SUM(total_reembolsado) AS total_gasto
            FROM main_marts.agg_senador_despesas
            GROUP BY ano, tipo_despesa
            ORDER BY ano, total_gasto DESC
        """).pl()


def get_ceaps_all_senators_totals() -> pl.DataFrame:
    """Total CEAPS spending per senator (all years) — used for ranking."""
    with _con() as con:
        return con.execute("""
            SELECT
                a.senador_id,
                COALESCE(s.nome_parlamentar, a.nome_senador) AS nome_parlamentar,
                s.partido_sigla,
                s.estado_sigla,
                SUM(a.total_reembolsado) AS total_gasto,
                SUM(a.qtd_recibos)       AS num_recibos
            FROM main_marts.agg_senador_despesas a
            LEFT JOIN main_marts.dim_senador s ON a.senador_id = s.senador_id
            GROUP BY a.senador_id, nome_parlamentar, a.nome_senador, s.partido_sigla, s.estado_sigla
            ORDER BY total_gasto DESC
        """).pl()


def get_senator_housing(senador_id: str) -> pl.DataFrame:
    """Housing allowance record for a senator."""
    with _con() as con:
        return con.execute("""
            SELECT
                auxilio_moradia,
                imovel_funcional
            FROM main_marts.fct_auxilio_moradia
            WHERE senador_id = ?
        """, [senador_id]).pl()


# ── Staff & Payroll (Servidores) queries ────────────────────────────────────

def get_pessoal_kpis() -> dict:
    """Key indicators for the staff payroll page: active staff, pensioners, latest month totals."""
    with _con() as con:
        servidores = con.execute("""
            SELECT COUNT(*) FROM main_marts.dim_servidor WHERE situacao = 'ATIVO'
        """).fetchone()[0]

        pensionistas = con.execute("""
            SELECT COUNT(*) FROM main_marts.dim_pensionista
        """).fetchone()[0]

        # Latest full month available in the payroll data
        latest = con.execute("""
            SELECT ano, mes, SUM(total_liquido) AS total_liquido, SUM(total_bruto) AS total_bruto
            FROM main_marts.agg_pessoal_mensal
            WHERE (ano, mes) IN (
                SELECT ano, mes FROM main_marts.agg_pessoal_mensal
                ORDER BY ano DESC, mes DESC LIMIT 1
            )
            GROUP BY ano, mes
        """).pl()

        pensionistas_latest = con.execute("""
            SELECT COALESCE(SUM(remuneracao_liquida), 0)
            FROM main_marts.fct_remuneracao_pensionista
            WHERE (ano, mes) IN (
                SELECT ano, mes FROM main_marts.fct_remuneracao_pensionista
                ORDER BY ano DESC, mes DESC LIMIT 1
            )
        """).fetchone()[0]

        horas_latest = con.execute("""
            SELECT COALESCE(SUM(valor_total), 0)
            FROM main_marts.fct_hora_extra
            WHERE (ano_pagamento, mes_pagamento) IN (
                SELECT ano_pagamento, mes_pagamento FROM main_marts.fct_hora_extra
                ORDER BY ano_pagamento DESC, mes_pagamento DESC LIMIT 1
            )
        """).fetchone()[0]

        # Average net salary for latest month (tipo_folha = 'Normal' avoids double-counting)
        avg_latest = con.execute("""
            SELECT COALESCE(AVG(remuneracao_liquida), 0)
            FROM main_marts.fct_remuneracao_servidor
            WHERE (ano, mes) IN (
                SELECT ano, mes FROM main_marts.fct_remuneracao_servidor
                ORDER BY ano DESC, mes DESC LIMIT 1
            )
            AND tipo_folha = 'Normal'
        """).fetchone()[0]

    result = {
        "num_servidores_ativos": servidores,
        "num_pensionistas": pensionistas,
        "total_liquido_mes": float(latest["total_liquido"][0]) if len(latest) > 0 else 0,
        "total_bruto_mes": float(latest["total_bruto"][0]) if len(latest) > 0 else 0,
        "total_pensionistas_mes": float(pensionistas_latest or 0),
        "total_horas_extras_mes": float(horas_latest or 0),
        "avg_remuneracao_mes": float(avg_latest or 0),
        "ano_ref": int(latest["ano"][0]) if len(latest) > 0 else 0,
        "mes_ref": int(latest["mes"][0]) if len(latest) > 0 else 0,
    }
    return result


def get_remuneracao_trend() -> pl.DataFrame:
    """Monthly payroll trend (staff + pensioners), 2019-present."""
    with _con() as con:
        servidores = con.execute("""
            SELECT
                ano,
                mes,
                data_competencia,
                SUM(total_liquido) AS total_liquido_servidores,
                SUM(total_bruto)   AS total_bruto_servidores,
                SUM(num_servidores) AS num_servidores
            FROM main_marts.agg_pessoal_mensal
            GROUP BY ano, mes, data_competencia
            ORDER BY ano, mes
        """).pl()

        pensionistas = con.execute("""
            SELECT
                ano,
                mes,
                SUM(remuneracao_liquida) AS total_liquido_pensionistas
            FROM main_marts.fct_remuneracao_pensionista
            GROUP BY ano, mes
            ORDER BY ano, mes
        """).pl()

    return servidores.join(pensionistas, on=["ano", "mes"], how="left")


def get_servidores_por_vinculo(ano: int, mes: int) -> pl.DataFrame:
    """Staff count and total payroll broken down by employment bond type for a given month."""
    with _con() as con:
        return con.execute("""
            SELECT
                vinculo,
                SUM(num_servidores) AS num_servidores,
                SUM(total_liquido)  AS total_liquido,
                SUM(total_bruto)    AS total_bruto
            FROM main_marts.agg_pessoal_mensal
            WHERE ano = ? AND mes = ?
            GROUP BY vinculo
            ORDER BY total_bruto DESC
        """, [ano, mes]).pl()


def get_top_remuneracoes(ano: int, mes: int, n: int = 20) -> pl.DataFrame:
    """Top N staff earners for a given month by net pay."""
    with _con() as con:
        return con.execute("""
            SELECT
                f.nome,
                f.sequencial,
                f.remuneracao_liquida,
                f.remuneracao_bruta,
                f.remuneracao_basica,
                f.funcao_comissionada,
                f.vantagens_indenizatorias,
                f.tipo_folha,
                COALESCE(f.lotacao_sigla, 'N/D') AS lotacao_sigla,
                COALESCE(f.lotacao_nome,  'N/D') AS lotacao_nome,
                COALESCE(f.vinculo,       'N/D') AS vinculo,
                COALESCE(f.cargo_nome,    'N/D') AS cargo_nome
            FROM main_marts.fct_remuneracao_servidor f
            WHERE f.ano = ? AND f.mes = ?
            ORDER BY f.remuneracao_liquida DESC
            LIMIT ?
        """, [ano, mes, n]).pl()


def get_remuneracao_componentes(ano: int, mes: int) -> pl.DataFrame:
    """Sum of each pay component for a given month — used for stacked breakdown chart."""
    with _con() as con:
        return con.execute("""
            SELECT
                SUM(remuneracao_basica)        AS remuneracao_basica,
                SUM(vantagens_pessoais)        AS vantagens_pessoais,
                SUM(funcao_comissionada)       AS funcao_comissionada,
                SUM(gratificacao_natalina)     AS gratificacao_natalina,
                SUM(horas_extras)              AS horas_extras,
                SUM(outras_eventuais)          AS outras_eventuais,
                SUM(diarias)                   AS diarias,
                SUM(auxilios)                  AS auxilios,
                SUM(abono_permanencia)         AS abono_permanencia,
                SUM(vantagens_indenizatorias)  AS vantagens_indenizatorias
            FROM main_marts.fct_remuneracao_servidor
            WHERE ano = ? AND mes = ?
        """, [ano, mes]).pl()


def get_lotacoes_top(ano: int, mes: int, n: int = 15) -> pl.DataFrame:
    """Top N organizational units by total payroll for a given month."""
    with _con() as con:
        return con.execute("""
            SELECT
                lotacao_sigla,
                lotacao_nome,
                SUM(num_servidores) AS num_servidores,
                SUM(total_liquido)  AS total_liquido,
                SUM(total_bruto)    AS total_bruto
            FROM main_marts.agg_pessoal_mensal
            WHERE ano = ? AND mes = ?
              AND lotacao_sigla != 'NÃO INFORMADO'
            GROUP BY lotacao_sigla, lotacao_nome
            ORDER BY total_bruto DESC
            LIMIT ?
        """, [ano, mes, n]).pl()


def get_pensionistas_trend() -> pl.DataFrame:
    """Monthly pensioner payroll trend, 2019-present."""
    with _con() as con:
        return con.execute("""
            SELECT
                ano,
                mes,
                data_competencia,
                COUNT(DISTINCT sequencial) AS num_pensionistas,
                SUM(remuneracao_liquida)   AS total_liquido,
                SUM(remuneracao_bruta)     AS total_bruto
            FROM main_marts.fct_remuneracao_pensionista
            GROUP BY ano, mes, data_competencia
            ORDER BY ano, mes
        """).pl()


def get_top_pensionistas(ano: int, mes: int, n: int = 10) -> pl.DataFrame:
    """Top N pensioners by net pay for a given month."""
    with _con() as con:
        return con.execute("""
            SELECT
                f.nome,
                f.sequencial,
                f.remuneracao_liquida,
                f.remuneracao_bruta,
                f.remuneracao_basica,
                f.tipo_folha,
                COALESCE(p.nome_instituidor, 'N/D') AS nome_instituidor,
                COALESCE(p.vinculo, 'N/D')          AS vinculo,
                COALESCE(p.cargo_nome, 'N/D')       AS cargo_nome
            FROM main_marts.fct_remuneracao_pensionista f
            LEFT JOIN main_marts.dim_pensionista p USING (sequencial)
            WHERE f.ano = ? AND f.mes = ?
            ORDER BY f.remuneracao_liquida DESC
            LIMIT ?
        """, [ano, mes, n]).pl()


def get_horas_extras_trend() -> pl.DataFrame:
    """Monthly overtime payments trend, 2019-present."""
    with _con() as con:
        return con.execute("""
            SELECT
                ano_pagamento                  AS ano,
                mes_pagamento                  AS mes,
                data_competencia,
                COUNT(DISTINCT sequencial)     AS num_servidores,
                SUM(valor_total)               AS total_valor
            FROM main_marts.fct_hora_extra
            GROUP BY ano_pagamento, mes_pagamento, data_competencia
            ORDER BY ano_pagamento, mes_pagamento
        """).pl()


def get_horas_extras_por_lotacao(ano: int, mes: int, n: int = 15) -> pl.DataFrame:
    """Top N organizational units by overtime value for a given month."""
    with _con() as con:
        return con.execute("""
            SELECT
                COALESCE(lotacao_sigla, 'N/D') AS lotacao_sigla,
                COALESCE(lotacao_nome,  'N/D') AS lotacao_nome,
                COUNT(DISTINCT sequencial)     AS num_servidores,
                SUM(valor_total)               AS total_valor
            FROM main_marts.fct_hora_extra
            WHERE ano_pagamento = ? AND mes_pagamento = ?
            GROUP BY lotacao_sigla, lotacao_nome
            ORDER BY total_valor DESC
            LIMIT ?
        """, [ano, mes, n]).pl()


def get_remuneracoes_anos_disponiveis() -> list[int]:
    """List of distinct years available in the staff payroll data."""
    with _con() as con:
        rows = con.execute("""
            SELECT DISTINCT ano FROM main_marts.agg_pessoal_mensal ORDER BY ano DESC
        """).fetchall()
    return [r[0] for r in rows]


def get_remuneracoes_meses_disponiveis(ano: int) -> list[int]:
    """List of distinct months available for a given year in the staff payroll data."""
    with _con() as con:
        rows = con.execute("""
            SELECT DISTINCT mes FROM main_marts.agg_pessoal_mensal WHERE ano = ? ORDER BY mes DESC
        """, [ano]).fetchall()
    return [r[0] for r in rows]


def get_remuneracao_por_ano() -> pl.DataFrame:
    """Annual payroll totals across all available years — used for the top-level overview chart."""
    with _con() as con:
        return con.execute("""
            WITH mensal AS (
                SELECT
                    ano,
                    mes,
                    SUM(num_servidores) AS servidores_mes,
                    SUM(total_liquido)  AS liquido_mes,
                    SUM(total_bruto)    AS bruto_mes
                FROM main_marts.agg_pessoal_mensal
                GROUP BY ano, mes
            )
            SELECT
                ano,
                COUNT(*)                                 AS num_meses,
                ROUND(AVG(servidores_mes))::INTEGER      AS avg_servidores,
                SUM(liquido_mes)                         AS total_liquido,
                SUM(bruto_mes)                           AS total_bruto
            FROM mensal
            GROUP BY ano
            ORDER BY ano
        """).pl()


def get_remuneracao_mensal_por_ano(ano: int) -> pl.DataFrame:
    """Monthly payroll totals for a given year — used for the monthly breakdown chart."""
    with _con() as con:
        return con.execute("""
            SELECT
                mes,
                SUM(num_servidores)           AS num_servidores,
                SUM(total_liquido)            AS total_liquido,
                SUM(total_bruto)              AS total_bruto,
                SUM(total_horas_extras_valor) AS total_horas_extras
            FROM main_marts.agg_pessoal_mensal
            WHERE ano = ?
            GROUP BY mes
            ORDER BY mes
        """, [ano]).pl()


def get_vinculo_por_ano(ano: int) -> pl.DataFrame:
    """Staff payroll breakdown by employment bond type for an entire year."""
    with _con() as con:
        return con.execute("""
            SELECT
                vinculo,
                SUM(num_servidores) / COUNT(DISTINCT mes)  AS avg_servidores,
                SUM(total_liquido)                         AS total_liquido,
                SUM(total_bruto)                           AS total_bruto
            FROM main_marts.agg_pessoal_mensal
            WHERE ano = ?
            GROUP BY vinculo
            ORDER BY total_bruto DESC
        """, [ano]).pl()


# ── Emendas Parlamentares ────────────────────────────────────────────────────

# 26 states + DF; used to exclude '-1' / 'EX' / null sentinel values
_VALID_UFS = (
    "'AC','AL','AP','AM','BA','CE','DF','ES','GO','MA',"
    "'MT','MS','MG','PA','PB','PR','PE','PI','RJ','RN',"
    "'RS','RO','RR','SC','SP','SE','TO'"
)

def get_emendas_kpis() -> dict:
    """Top-level KPIs for the emendas dashboard page."""
    with _con() as con:
        row = con.execute("""
            SELECT
                sum(num_emendas)                    as total_emendas,
                count(distinct nome_autor_emenda)   as total_autores,
                min(ano_emenda)                     as ano_min,
                max(ano_emenda)                     as ano_max,
                sum(total_pago)                     as total_pago_geral,
                sum(total_empenhado)                as total_empenhado_geral
            FROM main_marts.agg_emenda_por_autor
        """).fetchone()
    return {
        "total_emendas":       row[0] or 0,
        "total_autores":       row[1] or 0,
        "ano_min":             row[2] or 0,
        "ano_max":             row[3] or 0,
        "total_pago":          row[4] or 0.0,
        "total_empenhado":     row[5] or 0.0,
    }


def get_emendas_por_uf(
    ano_inicio: int | None = None,
    ano_fim: int | None = None,
) -> pl.DataFrame:
    """Total paid per Brazilian state (UF) — used for the choropleth map.

    Filters out sentinel values ('-1', 'EX', NULL) so only the 27 valid
    state codes appear.  Accepts optional year range to allow the user to
    slice by period.
    """
    params: list = []
    extra = ""
    if ano_inicio is not None:
        extra += " AND ano_emenda >= ?"
        params.append(ano_inicio)
    if ano_fim is not None:
        extra += " AND ano_emenda <= ?"
        params.append(ano_fim)

    with _con() as con:
        return con.execute(f"""
            SELECT
                uf_recurso,
                sum(valor_pago)                as total_pago,
                count(distinct codigo_emenda)  as num_emendas,
                count(distinct favorecido)     as num_favorecidos,
                count(distinct municipio_recurso) as num_municipios
            FROM main_marts.fct_emenda_documento
            WHERE fase_despesa = 'Pagamento'
              AND uf_recurso IN ({_VALID_UFS})
              {extra}
            GROUP BY uf_recurso
            ORDER BY total_pago DESC
        """, params).pl()


def get_emendas_por_uf_autoria(
    ano_inicio: int | None = None,
    ano_fim: int | None = None,
) -> pl.DataFrame:
    """Total paid grouped by the *author's* home state (UF do parlamentar).

    Uses agg_emenda_por_autor, which already carries estado_sigla from the
    dim_senador join.  Only authors successfully linked to a senator record
    are included.
    """
    params: list = []
    extra = ""
    if ano_inicio is not None:
        extra += " AND ano_emenda >= ?"
        params.append(ano_inicio)
    if ano_fim is not None:
        extra += " AND ano_emenda <= ?"
        params.append(ano_fim)

    with _con() as con:
        return con.execute(f"""
            SELECT
                estado_sigla                              as uf,
                sum(total_pago)                           as total_pago,
                sum(num_emendas)                          as num_emendas,
                count(distinct nome_autor_emenda)         as num_autores,
                sum(num_municipios_distintos)             as num_municipios
            FROM main_marts.agg_emenda_por_autor
            WHERE estado_sigla IS NOT NULL
              AND estado_sigla IN ({_VALID_UFS})
              {extra}
            GROUP BY estado_sigla
            ORDER BY total_pago DESC
        """, params).pl()


def get_emendas_por_ano() -> pl.DataFrame:
    """Annual totals of emendas — empenhado vs pago, all authors."""
    with _con() as con:
        return con.execute("""
            SELECT
                ano_emenda,
                sum(total_empenhado)              as total_empenhado,
                sum(total_pago)                   as total_pago,
                count(distinct nome_autor_emenda) as num_autores,
                sum(num_emendas)                  as num_emendas
            FROM main_marts.agg_emenda_por_autor
            GROUP BY ano_emenda
            ORDER BY ano_emenda
        """).pl()


def get_top_autores_emendas(n: int = 20) -> pl.DataFrame:
    """Top N authors by total paid across all years."""
    with _con() as con:
        return con.execute("""
            SELECT
                nome_autor_emenda,
                senador_id,
                nome_parlamentar,
                partido_sigla,
                estado_sigla,
                is_senador_atual,
                sum(total_pago)               as total_pago,
                sum(total_empenhado)          as total_empenhado,
                sum(num_emendas)              as num_emendas,
                sum(num_municipios_distintos) as municipios
            FROM main_marts.agg_emenda_por_autor
            GROUP BY nome_autor_emenda, senador_id, nome_parlamentar,
                     partido_sigla, estado_sigla, is_senador_atual
            ORDER BY total_pago DESC
            LIMIT ?
        """, [n]).pl()


def get_senator_emendas_kpis(senador_id: str) -> dict:
    """Lifetime emenda KPIs for a single senator.

    Uses conditional aggregation to derive empenhado and pago from their
    respective phases in a single scan (no fase_despesa filter to avoid
    losing values that only appear in one phase).
    """
    with _con() as con:
        row = con.execute("""
            SELECT
                count(distinct codigo_emenda)                                       as num_emendas,
                sum(case when fase_despesa = 'Pagamento' then valor_pago end)       as total_pago,
                sum(case when fase_despesa = 'Empenho'   then valor_empenhado end)  as total_empenhado,
                min(ano_emenda)                                                     as ano_min,
                max(ano_emenda)                                                     as ano_max,
                count(distinct case when fase_despesa = 'Pagamento'
                    then municipio_recurso end)                                     as municipios,
                count(distinct case when fase_despesa = 'Pagamento'
                    then codigo_favorecido end)                                     as favorecidos
            FROM main_marts.fct_emenda_documento
            WHERE senador_id = ?
        """, [senador_id]).fetchone()
    return {
        "num_emendas":     row[0] or 0,
        "total_pago":      row[1] or 0.0,
        "total_empenhado": row[2] or 0.0,
        "ano_min":         row[3],
        "ano_max":         row[4],
        "municipios":      row[5] or 0,
        "favorecidos":     row[6] or 0,
    }


def get_senator_emendas_por_ano(senador_id: str) -> pl.DataFrame:
    """Emenda payment totals by year for a senator."""
    with _con() as con:
        return con.execute("""
            SELECT
                ano_emenda,
                sum(valor_pago)               as total_pago,
                sum(valor_empenhado)          as total_empenhado,
                count(distinct codigo_emenda) as num_emendas
            FROM main_marts.fct_emenda_documento
            WHERE senador_id = ?
              AND fase_despesa = 'Pagamento'
            GROUP BY ano_emenda
            ORDER BY ano_emenda
        """, [senador_id]).pl()


def get_senator_emendas_favorecidos(senador_id: str, n: int = 15) -> pl.DataFrame:
    """Top N beneficiaries of a senator's emendas by total paid."""
    with _con() as con:
        return con.execute("""
            SELECT
                favorecido,
                codigo_favorecido,
                tipo_favorecido,
                municipio_favorecido,
                uf_favorecido,
                sum(valor_pago)  as total_pago,
                count(*)         as num_documentos
            FROM main_marts.fct_emenda_documento
            WHERE senador_id = ?
              AND fase_despesa = 'Pagamento'
              AND favorecido IS NOT NULL
            GROUP BY favorecido, codigo_favorecido, tipo_favorecido,
                     municipio_favorecido, uf_favorecido
            ORDER BY total_pago DESC
            LIMIT ?
        """, [senador_id, n]).pl()


def get_senator_emendas_municipios(senador_id: str) -> pl.DataFrame:
    """Geographic distribution of a senator's emenda payments by municipality."""
    with _con() as con:
        return con.execute("""
            SELECT
                uf_recurso,
                municipio_recurso,
                codigo_ibge_municipio,
                sum(valor_pago)               as total_pago,
                count(distinct codigo_emenda) as num_emendas
            FROM main_marts.fct_emenda_documento
            WHERE senador_id = ?
              AND fase_despesa = 'Pagamento'
              AND municipio_recurso IS NOT NULL
              AND municipio_recurso NOT IN ('Sem informação', '-1')
            GROUP BY uf_recurso, municipio_recurso, codigo_ibge_municipio
            ORDER BY total_pago DESC
        """, [senador_id]).pl()


# ── Deputies (Câmara) queries ──────────────────────────────────────────────

def get_all_deputies() -> pl.DataFrame:
    """All deputies in dim_deputado, sorted by name."""
    with _con() as con:
        return con.execute("""
            SELECT
                deputado_id,
                nome_parlamentar,
                nome_civil,
                sigla_partido,
                sigla_uf,
                situacao,
                em_exercicio,
                url_foto,
                legislatura_min,
                legislatura_max
            FROM main_marts.dim_deputado
            ORDER BY nome_parlamentar
        """).pl()


def get_deputy_by_id(deputado_id: str) -> pl.DataFrame:
    """Full dim_deputado row for a single deputy."""
    with _con() as con:
        return con.execute("""
            SELECT *
            FROM main_marts.dim_deputado
            WHERE deputado_id = ?
        """, [deputado_id]).pl()


def get_deputies_party_composition() -> pl.DataFrame:
    """Deputy count per party (active deputies only)."""
    with _con() as con:
        return con.execute("""
            SELECT
                sigla_partido,
                COUNT(*) AS num_deputados,
                SUM(CASE WHEN em_exercicio THEN 1 ELSE 0 END) AS em_exercicio
            FROM main_marts.dim_deputado
            GROUP BY sigla_partido
            ORDER BY num_deputados DESC
        """).pl()


def get_deputy_vote_summary(deputado_id: str) -> pl.DataFrame:
    """Aggregate vote-type counts for a deputy (for the accountability scorecard)."""
    with _con() as con:
        return con.execute("""
            SELECT
                COUNT(*)                                               AS total_votacoes,
                SUM(CASE WHEN voto_afirmativo   THEN 1 ELSE 0 END)   AS total_sim,
                SUM(CASE WHEN voto_negativo     THEN 1 ELSE 0 END)   AS total_nao,
                SUM(CASE WHEN presente_sem_voto THEN 1 ELSE 0 END)   AS total_abstencao,
                ROUND(
                    100.0 * SUM(CASE WHEN voto_afirmativo OR voto_negativo OR presente_sem_voto
                                     THEN 1 ELSE 0 END)
                    / NULLIF(COUNT(*), 0), 1
                ) AS taxa_presenca
            FROM main_marts.fct_votacao_deputado
            WHERE deputado_id = ?
        """, [deputado_id]).pl()


def get_deputy_votes(deputado_id: str, n: int = 200) -> pl.DataFrame:
    """Individual vote records for a deputy, most recent first."""
    with _con() as con:
        return con.execute("""
            SELECT
                data_votacao,
                proposicao_objeto,
                descricao,
                tipo_voto,
                voto_afirmativo,
                voto_negativo,
                presente_sem_voto,
                aprovacao,
                partido_sigla_voto,
                sigla_orgao
            FROM main_marts.fct_votacao_deputado
            WHERE deputado_id = ?
            ORDER BY data_votacao DESC
            LIMIT ?
        """, [deputado_id, n]).pl()


def get_deputy_expenses(deputado_id: str) -> pl.DataFrame:
    """All CEAP expense records for a deputy, including NF and supplier CNPJ."""
    with _con() as con:
        return con.execute("""
            SELECT
                data_documento,
                ano,
                mes,
                tipo_despesa,
                tipo_documento,
                num_documento,
                nome_fornecedor,
                cnpj_cpf_fornecedor,
                valor_documento,
                valor_glosa,
                valor_liquido,
                num_ressarcimento,
                url_documento
            FROM main_marts.fct_despesa_deputado
            WHERE deputado_id = ?
            ORDER BY ano DESC, mes DESC, data_documento DESC
        """, [deputado_id]).pl()


def get_deputy_proposals(deputado_id: str, n: int = 500) -> pl.DataFrame:
    """Legislative proposals authored by a deputy."""
    with _con() as con:
        return con.execute("""
            SELECT
                proposicao_id,
                sigla_tipo,
                numero,
                ano,
                ementa,
                data_apresentacao,
                descricao_situacao,
                regime_status,
                sigla_orgao_status,
                apreciacao,
                url_inteiro_teor
            FROM main_marts.fct_proposicao_deputado
            WHERE deputado_id_autor = ?
            ORDER BY ano DESC, data_apresentacao DESC
            LIMIT ?
        """, [deputado_id, n]).pl()


def get_deputy_proposals_summary(deputado_id: str) -> pl.DataFrame:
    """Proposals grouped by type and year for stacked bar chart."""
    with _con() as con:
        return con.execute("""
            SELECT
                ano,
                sigla_tipo,
                COUNT(*) AS num_proposicoes
            FROM main_marts.fct_proposicao_deputado
            WHERE deputado_id_autor = ?
            GROUP BY ano, sigla_tipo
            ORDER BY ano, num_proposicoes DESC
        """, [deputado_id]).pl()


def get_deputy_emendas_kpis(deputado_id: str) -> dict:
    """Lifetime emenda KPIs for a single deputy from fct_emenda_documento.deputado_id."""
    with _con() as con:
        row = con.execute("""
            SELECT
                count(distinct codigo_emenda)                                      AS num_emendas,
                sum(CASE WHEN fase_despesa='Pagamento' THEN valor_pago END)        AS total_pago,
                sum(CASE WHEN fase_despesa='Empenho'   THEN valor_empenhado END)   AS total_empenhado,
                min(ano_emenda)                                                    AS ano_min,
                max(ano_emenda)                                                    AS ano_max,
                count(DISTINCT CASE WHEN fase_despesa='Pagamento'
                    THEN municipio_recurso END)                                    AS municipios,
                count(DISTINCT CASE WHEN fase_despesa='Pagamento'
                    THEN codigo_favorecido END)                                    AS favorecidos
            FROM main_marts.fct_emenda_documento
            WHERE deputado_id = ?
        """, [deputado_id]).fetchone()
    return {
        "num_emendas":     row[0] or 0,
        "total_pago":      row[1] or 0.0,
        "total_empenhado": row[2] or 0.0,
        "ano_min":         row[3],
        "ano_max":         row[4],
        "municipios":      row[5] or 0,
        "favorecidos":     row[6] or 0,
    }


def get_deputy_emendas_por_ano(deputado_id: str) -> pl.DataFrame:
    """Emenda payment totals by year for a deputy."""
    with _con() as con:
        return con.execute("""
            SELECT
                ano_emenda,
                sum(valor_pago)               AS total_pago,
                sum(valor_empenhado)          AS total_empenhado,
                count(distinct codigo_emenda) AS num_emendas
            FROM main_marts.fct_emenda_documento
            WHERE deputado_id = ?
              AND fase_despesa = 'Pagamento'
            GROUP BY ano_emenda
            ORDER BY ano_emenda
        """, [deputado_id]).pl()


def get_deputy_emendas_favorecidos(deputado_id: str, n: int = 15) -> pl.DataFrame:
    """Top N beneficiaries of a deputy's emendas by total paid."""
    with _con() as con:
        return con.execute("""
            SELECT
                favorecido,
                codigo_favorecido,
                tipo_favorecido,
                municipio_favorecido,
                uf_favorecido,
                sum(valor_pago) AS total_pago,
                count(*)        AS num_documentos
            FROM main_marts.fct_emenda_documento
            WHERE deputado_id = ?
              AND fase_despesa = 'Pagamento'
              AND favorecido IS NOT NULL
            GROUP BY favorecido, codigo_favorecido, tipo_favorecido,
                     municipio_favorecido, uf_favorecido
            ORDER BY total_pago DESC
            LIMIT ?
        """, [deputado_id, n]).pl()


def get_deputy_emendas_municipios(deputado_id: str) -> pl.DataFrame:
    """Geographic distribution of a deputy's emenda payments by municipality."""
    with _con() as con:
        return con.execute("""
            SELECT
                uf_recurso,
                municipio_recurso,
                codigo_ibge_municipio,
                sum(valor_pago)               AS total_pago,
                count(distinct codigo_emenda) AS num_emendas
            FROM main_marts.fct_emenda_documento
            WHERE deputado_id = ?
              AND fase_despesa = 'Pagamento'
              AND municipio_recurso IS NOT NULL
              AND municipio_recurso NOT IN ('Sem informação', '-1')
            GROUP BY uf_recurso, municipio_recurso, codigo_ibge_municipio
            ORDER BY total_pago DESC
        """, [deputado_id]).pl()


def get_deputy_expenses_ranking(n: int = 20) -> pl.DataFrame:
    """Top N deputies by total CEAP expense across all years."""
    with _con() as con:
        return con.execute("""
            SELECT
                f.deputado_id,
                COALESCE(ANY_VALUE(d.nome_parlamentar), ANY_VALUE(f.nome_parlamentar_dim)) AS nome_parlamentar,
                ANY_VALUE(d.sigla_partido) AS sigla_partido,
                ANY_VALUE(d.sigla_uf)     AS sigla_uf,
                SUM(f.valor_liquido)       AS total_gasto,
                COUNT(*)                   AS num_documentos
            FROM main_marts.fct_despesa_deputado f
            LEFT JOIN main_marts.dim_deputado d USING (deputado_id)
            GROUP BY f.deputado_id
            ORDER BY total_gasto DESC
            LIMIT ?
        """, [n]).pl()


def get_senator_apoiamentos(senador_id: str) -> pl.DataFrame:
    """Commitments co-sponsored (apoiados) by a senator."""
    with _con() as con:
        return con.execute("""
            SELECT
                empenho,
                codigo_emenda,
                ano_emenda,
                tipo_emenda,
                nome_autor_emenda,
                data_apoio,
                favorecido,
                uf_favorecido,
                municipio_favorecido,
                orgao,
                acao,
                valor_empenhado,
                valor_pago
            FROM main_marts.fct_apoiamento_emenda
            WHERE senador_id_apoiador = ?
            ORDER BY data_apoio DESC
        """, [senador_id]).pl()


# ── New analysis queries ─────────────────────────────────────────────────────

def get_remuneracao_distribuicao(ano: int, mes: int) -> pl.DataFrame:
    """All individual staff salaries for a given month — used for box plot and outlier detection."""
    with _con() as con:
        return con.execute("""
            SELECT
                sequencial,
                nome,
                vinculo,
                cargo_nome,
                lotacao_sigla,
                remuneracao_liquida,
                remuneracao_basica
            FROM main_marts.fct_remuneracao_servidor
            WHERE ano = ? AND mes = ? AND tipo_folha = 'Normal'
              AND remuneracao_liquida IS NOT NULL
            ORDER BY remuneracao_liquida DESC
        """, [ano, mes]).pl()


def get_ceaps_raw_receipts(ano: int | None = None) -> pl.DataFrame:
    """Individual CEAPS receipt records for outlier detection.

    Returns up to 5000 rows ordered by value descending so callers can compute
    per-category medians and flag statistical outliers.
    """
    with _con() as con:
        if ano is None:
            return con.execute("""
                SELECT
                    c.senador_id,
                    c.nome_senador,
                    c.ano,
                    c.mes,
                    c.tipo_despesa,
                    c.fornecedor,
                    c.cnpj_cpf,
                    c.data,
                    c.valor_reembolsado
                FROM main_marts.fct_ceaps c
                WHERE c.valor_reembolsado > 0
                ORDER BY c.valor_reembolsado DESC
                LIMIT 5000
            """).pl()
        else:
            return con.execute("""
                SELECT
                    c.senador_id,
                    c.nome_senador,
                    c.ano,
                    c.mes,
                    c.tipo_despesa,
                    c.fornecedor,
                    c.cnpj_cpf,
                    c.data,
                    c.valor_reembolsado
                FROM main_marts.fct_ceaps c
                WHERE c.ano = ? AND c.valor_reembolsado > 0
                ORDER BY c.valor_reembolsado DESC
                LIMIT 5000
            """, [ano]).pl()


def get_votacao_tramitacao() -> pl.DataFrame:
    """Deliberation timeline per Senate bill: days between first and last plenário vote.

    Groups fct_votacao by materia_identificacao to compute min/max vote dates.
    Returns one row per bill, ordered by deliberation time (longest first).
    """
    with _con() as con:
        return con.execute("""
            WITH sessions AS (
                SELECT
                    codigo_sessao_votacao,
                    sigla_materia,
                    materia_identificacao,
                    materia_ementa,
                    resultado_votacao,
                    MAX(data_sessao)           AS data_sessao,
                    MAX(total_votos_sim)       AS total_votos_sim,
                    MAX(total_votos_nao)       AS total_votos_nao
                FROM main_marts.fct_votacao
                GROUP BY codigo_sessao_votacao, sigla_materia,
                         materia_identificacao, materia_ementa, resultado_votacao
            )
            SELECT
                sigla_materia,
                materia_identificacao,
                MAX(materia_ementa)                                         AS materia_ementa,
                COUNT(DISTINCT codigo_sessao_votacao)                       AS num_sessoes,
                MIN(data_sessao)                                            AS primeira_sessao,
                MAX(data_sessao)                                            AS ultima_sessao,
                DATEDIFF('day', MIN(data_sessao), MAX(data_sessao))        AS dias_deliberacao,
                LAST(resultado_votacao ORDER BY data_sessao)                AS resultado_final,
                MAX(total_votos_sim)                                        AS max_sim,
                MAX(total_votos_nao)                                        AS max_nao,
                ABS(MAX(total_votos_sim) - MAX(total_votos_nao))           AS margem
            FROM sessions
            WHERE materia_identificacao IS NOT NULL
              AND sigla_materia IS NOT NULL
            GROUP BY sigla_materia, materia_identificacao
            ORDER BY dias_deliberacao DESC
        """).pl()


def get_deputy_emendas_kpis_by_name(nome_parlamentar: str) -> dict:
    """Fallback emenda KPIs for a deputy using name-based search.

    Used when deputado_id linkage in dim_emenda returns no results.
    Normalizes the name (uppercase, no accents via unaccent-style comparison)
    and searches nome_autor_emenda with a LIKE pattern.
    """
    # Normalize: uppercase, strip extra spaces — DuckDB lacks unaccent, so use LIKE
    nome_upper = nome_parlamentar.upper().strip()
    # Use first two words of the parliamentary name as the LIKE pattern for robustness
    words = nome_upper.split()
    pattern = "%" + words[0] + "%" if words else "%"

    with _con() as con:
        row = con.execute("""
            SELECT
                count(distinct codigo_emenda)                                    AS num_emendas,
                sum(CASE WHEN fase_despesa='Pagamento' THEN valor_pago END)     AS total_pago,
                sum(CASE WHEN fase_despesa='Empenho'   THEN valor_empenhado END) AS total_empenhado,
                min(ano_emenda)                                                  AS ano_min,
                max(ano_emenda)                                                  AS ano_max,
                count(DISTINCT CASE WHEN fase_despesa='Pagamento'
                    THEN municipio_recurso END)                                  AS municipios,
                count(DISTINCT CASE WHEN fase_despesa='Pagamento'
                    THEN favorecido END)                                          AS favorecidos
            FROM main_marts.fct_emenda_documento
            WHERE UPPER(nome_autor_emenda) LIKE ?
        """, [pattern]).fetchone()

    return {
        "num_emendas":     row[0] or 0,
        "total_pago":      row[1] or 0.0,
        "total_empenhado": row[2] or 0.0,
        "ano_min":         row[3],
        "ano_max":         row[4],
        "municipios":      row[5] or 0,
        "favorecidos":     row[6] or 0,
    }
