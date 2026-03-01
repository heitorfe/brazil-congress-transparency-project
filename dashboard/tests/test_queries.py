"""Smoke tests for new dashboard query functions.

Verifies connectivity to DuckDB and that each new function returns
the expected shape and column structure. Run from repo root:

    cd dashboard
    ../.venv/Scripts/python.exe -m pytest tests/ -v
"""
import sys
import os

# Ensure the dashboard directory is in path so queries.py is importable
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import polars as pl
import pytest

from queries import (
    get_remuneracao_distribuicao,
    get_ceaps_raw_receipts,
    get_votacao_tramitacao,
    get_deputy_emendas_kpis_by_name,
    get_pessoal_kpis,
)


def test_pessoal_kpis_includes_avg_remuneracao():
    """get_pessoal_kpis() must include the new avg_remuneracao_mes key."""
    kpis = get_pessoal_kpis()
    assert "avg_remuneracao_mes" in kpis, "avg_remuneracao_mes key missing from kpis dict"
    assert isinstance(kpis["avg_remuneracao_mes"], float)


def test_remuneracao_distribuicao_returns_dataframe():
    """get_remuneracao_distribuicao() must return a non-empty DataFrame for a valid month."""
    # Use the most recent available year/month known in the dataset
    df = get_remuneracao_distribuicao(2024, 12)
    assert isinstance(df, pl.DataFrame)
    assert "remuneracao_liquida" in df.columns
    assert "vinculo" in df.columns
    assert "nome" in df.columns
    # Most salaries are positive; negative values are valid correction entries
    # (deductions exceeding gross pay). Assert the max is positive, not min.
    assert df["remuneracao_liquida"].max() > 0


def test_remuneracao_distribuicao_empty_on_missing_month():
    """get_remuneracao_distribuicao() must return empty (not error) for a month with no data."""
    df = get_remuneracao_distribuicao(2000, 1)
    assert isinstance(df, pl.DataFrame)
    assert len(df) == 0


def test_ceaps_raw_receipts_with_year():
    """get_ceaps_raw_receipts(ano) must return a DataFrame with value columns."""
    df = get_ceaps_raw_receipts(2024)
    assert isinstance(df, pl.DataFrame)
    assert "valor_reembolsado" in df.columns
    assert "tipo_despesa" in df.columns
    assert "nome_senador" in df.columns
    assert len(df) > 0


def test_ceaps_raw_receipts_without_year():
    """get_ceaps_raw_receipts(None) fetches all years."""
    df = get_ceaps_raw_receipts(None)
    assert isinstance(df, pl.DataFrame)
    assert len(df) > 0
    # Should contain multiple years
    assert df["ano"].n_unique() >= 2


def test_votacao_tramitacao_returns_expected_columns():
    """get_votacao_tramitacao() must contain all columns required by the dashboard."""
    df = get_votacao_tramitacao()
    assert isinstance(df, pl.DataFrame)
    required_cols = ["sigla_materia", "materia_identificacao", "dias_deliberacao",
                     "num_sessoes", "primeira_sessao", "ultima_sessao", "margem"]
    for col in required_cols:
        assert col in df.columns, f"Missing column: {col}"
    assert len(df) > 0
    # Deliberation days must be non-negative
    assert df["dias_deliberacao"].min() >= 0


def test_votacao_tramitacao_contains_pec():
    """Tramitation data should include PEC bills (common matter type in Senate)."""
    df = get_votacao_tramitacao()
    tipos = df["sigla_materia"].unique().to_list()
    assert "PEC" in tipos, "PEC not found in tramitation data — data may be empty or misjoined"


def test_deputy_emendas_kpis_by_name_returns_dict():
    """get_deputy_emendas_kpis_by_name() must always return a valid dict, even for no-match."""
    result = get_deputy_emendas_kpis_by_name("NOME INEXISTENTE XYZABC")
    assert isinstance(result, dict)
    assert "num_emendas" in result
    assert result["num_emendas"] == 0  # Should be 0 for a non-existent name


def test_deputy_emendas_kpis_by_name_finds_common_name():
    """A common partial name should return some emendas if the dataset is populated."""
    # "SILVA" is extremely common in Brazilian names — should hit at least one emenda author
    result = get_deputy_emendas_kpis_by_name("SILVA")
    assert isinstance(result, dict)
    assert "num_emendas" in result
    # We can't assert > 0 without knowing the data, but the function should not raise
