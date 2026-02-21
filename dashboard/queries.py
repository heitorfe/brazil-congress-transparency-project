"""
Thin query layer — all DuckDB interactions live here.
Dashboard pages import from this module; they never touch DuckDB directly.
"""

import duckdb
import polars as pl
from pathlib import Path

DB_PATH = Path("data/warehouse/senate.duckdb")


def _con() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(DB_PATH), read_only=True)


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
            FROM marts.dim_senador
            WHERE em_exercicio = true
            ORDER BY nome_parlamentar
        """).pl()


def get_senator_by_id(senador_id: str) -> pl.DataFrame:
    """Single senator row by ID."""
    with _con() as con:
        return con.execute("""
            SELECT *
            FROM marts.dim_senador
            WHERE senador_id = ?
        """, [senador_id]).pl()


def get_parties() -> list[str]:
    """Sorted list of party siglas with at least one senator in office."""
    with _con() as con:
        rows = con.execute("""
            SELECT partido_sigla FROM marts.dim_partido ORDER BY partido_sigla
        """).fetchall()
    return [r[0] for r in rows]
