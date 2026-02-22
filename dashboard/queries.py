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
