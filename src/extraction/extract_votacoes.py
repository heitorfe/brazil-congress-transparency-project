"""
Extract nominal voting records from the Brazilian Senate Open Data API.

Endpoint used:
  GET /votacao?dataInicio=YYYY-MM-DD&dataFim=YYYY-MM-DD
  -- Returns all plenary voting sessions in the date range, with all senator votes
     nested inside each session object.

Strategy:
  - Queries month-by-month from START_DATE to today (or a supplied end date).
  - Each API response is a JSON array; votes are embedded in each session.
  - Session-level data → data/raw/votacoes.parquet
  - Senator-level vote data → data/raw/votos.parquet  (exploded from nested votos[])

See docs/raw_data_schemas.md for the full field-by-field schema documentation.
"""

import sys
import time
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta
import httpx
import polars as pl
from pathlib import Path

if sys.stdout.encoding != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

BASE_URL = "https://legis.senado.leg.br/dadosabertos"
RAW_DIR = Path("data/raw")

# Earliest date with reliable data in the new endpoint
START_DATE = date(2019, 2, 1)


def _get(client: httpx.Client, url: str, params: dict) -> list[dict] | dict:
    resp = client.get(url, params=params, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    # The endpoint returns a plain JSON array for date-range queries
    if isinstance(data, list):
        return data
    # v=1 wraps the result; handle defensively
    if isinstance(data, dict) and "votacoes" in data:
        return data["votacoes"]
    return data


def _month_windows(start: date, end: date) -> list[tuple[date, date]]:
    """Yield (window_start, window_end) tuples, one per calendar month."""
    windows = []
    cursor = start.replace(day=1)
    while cursor <= end:
        next_month = cursor + relativedelta(months=1)
        window_end = min(next_month - timedelta(days=1), end)
        windows.append((cursor, window_end))
        cursor = next_month
    return windows


def _flatten_votacao(v: dict) -> dict:
    """Extract session-level fields, discarding the nested votos list."""
    inf = v.get("informeLegislativo") or {}
    return {
        "codigo_sessao_votacao":    v.get("codigoSessaoVotacao"),
        "codigo_votacao_sve":       v.get("codigoVotacaoSve"),
        "codigo_sessao":            v.get("codigoSessao"),
        "codigo_sessao_legislativa": v.get("codigoSessaoLegislativa"),
        "sigla_tipo_sessao":        v.get("siglaTipoSessao"),
        "numero_sessao":            v.get("numeroSessao"),
        "data_sessao":              v.get("dataSessao"),
        "id_processo":              v.get("idProcesso"),
        "codigo_materia":           v.get("codigoMateria"),
        "identificacao":            v.get("identificacao"),
        "sigla_materia":            v.get("sigla"),
        "numero_materia":           str(v.get("numero") or ""),
        "ano_materia":              v.get("ano"),
        "data_apresentacao":        v.get("dataApresentacao"),
        "ementa":                   v.get("ementa"),
        "sequencial_sessao":        v.get("sequencialSessao"),
        "votacao_secreta":          v.get("votacaoSecreta"),
        "descricao_votacao":        v.get("descricaoVotacao"),
        "resultado_votacao":        v.get("resultadoVotacao"),
        "total_votos_sim":          v.get("totalVotosSim"),
        "total_votos_nao":          v.get("totalVotosNao"),
        "total_votos_abstencao":    v.get("totalVotosAbstencao"),
        "informe_texto":            inf.get("texto"),
    }


def _flatten_voto(codigo_sessao_votacao: int, voto: dict) -> dict:
    """Extract one senator's vote from the nested votos array."""
    return {
        "codigo_sessao_votacao": codigo_sessao_votacao,
        "codigo_parlamentar":    voto.get("codigoParlamentar"),
        "nome_parlamentar":      voto.get("nomeParlamentar"),
        "sexo_parlamentar":      voto.get("sexoParlamentar"),
        "sigla_partido":         voto.get("siglaPartidoParlamentar"),
        "sigla_uf":              voto.get("siglaUFParlamentar"),
        "sigla_voto":            voto.get("siglaVotoParlamentar"),
        "descricao_voto":        voto.get("descricaoVotoParlamentar"),
    }


def fetch_window(
    client: httpx.Client, window_start: date, window_end: date
) -> tuple[list[dict], list[dict]]:
    """Fetch all voting sessions for one date window; return (votacoes, votos)."""
    sessions = _get(
        client,
        f"{BASE_URL}/votacao",
        params={
            "dataInicio": window_start.isoformat(),
            "dataFim": window_end.isoformat(),
        },
    )

    if not isinstance(sessions, list):
        # Single-session edge case from v=1 style response
        sessions = [sessions]

    votacoes = []
    votos = []
    for session in sessions:
        if not session or not isinstance(session, dict):
            continue
        codigo = session.get("codigoSessaoVotacao")
        if codigo is None:
            continue
        votacoes.append(_flatten_votacao(session))
        for voto in session.get("votos") or []:
            votos.append(_flatten_voto(codigo, voto))

    return votacoes, votos


def extract_all(start: date = START_DATE, end: date | None = None):
    if end is None:
        end = date.today()

    RAW_DIR.mkdir(parents=True, exist_ok=True)

    windows = _month_windows(start, end)
    print(f"Fetching {len(windows)} monthly windows from {start} to {end}...")

    all_votacoes: list[dict] = []
    all_votos: list[dict] = []

    with httpx.Client() as client:
        for i, (w_start, w_end) in enumerate(windows, 1):
            label = f"[{i:>3}/{len(windows)}] {w_start} → {w_end}"
            try:
                votacoes, votos = fetch_window(client, w_start, w_end)
                all_votacoes.extend(votacoes)
                all_votos.extend(votos)
                print(f"  {label}  sessions={len(votacoes):>4}  votes={len(votos):>5}")
            except Exception as e:
                print(f"  {label}  ERROR: {e}")
            time.sleep(0.3)

    if not all_votacoes:
        print("No voting data fetched. Exiting.")
        return

    df_votacoes = pl.DataFrame(all_votacoes).unique(subset=["codigo_sessao_votacao"])
    df_votos = pl.DataFrame(all_votos).unique(
        subset=["codigo_sessao_votacao", "codigo_parlamentar"]
    )

    df_votacoes.write_parquet(RAW_DIR / "votacoes.parquet")
    df_votos.write_parquet(RAW_DIR / "votos.parquet")

    print(f"\nSaved {len(df_votacoes)} voting sessions -> data/raw/votacoes.parquet")
    print(f"Saved {len(df_votos)} senator votes    -> data/raw/votos.parquet")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Extract Senate nominal votes")
    parser.add_argument(
        "--start",
        default=START_DATE.isoformat(),
        help=f"Start date YYYY-MM-DD (default: {START_DATE})",
    )
    parser.add_argument(
        "--end",
        default=None,
        help="End date YYYY-MM-DD (default: today)",
    )
    args = parser.parse_args()

    extract_all(
        start=date.fromisoformat(args.start),
        end=date.fromisoformat(args.end) if args.end else None,
    )
