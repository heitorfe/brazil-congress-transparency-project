"""
Extract housing allowance (Auxílio-Moradia) data for Brazilian senators from the ADM API.

Endpoint used:
  GET /api/v1/senadores/auxilio-moradia
  -- Returns a flat snapshot of all senators with boolean flags for housing benefits.
  -- ~86 records total (one per senator).

Strategy:
  - Single API call — no pagination or date windowing needed.
  - Output: data/raw/auxilio_moradia.parquet

Key quirks:
  - No senator ID in the response — only `nomeParlamentar` (name) is available.
    Matching to dim_senador must be done by name in the dbt layer.
  - Boolean fields arrive as Python booleans from the JSON parser.
"""

import sys
from pathlib import Path

import polars as pl

from api_client import SenateApiClient

if sys.stdout.encoding != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

RAW_DIR = Path("data/raw")


def _flatten_record(rec: dict) -> dict:
    return {
        "nome_parlamentar": rec.get("nomeParlamentar"),
        "estado_eleito":    rec.get("estadoEleito"),
        "partido_eleito":   rec.get("partidoEleito"),
        "auxilio_moradia":  bool(rec.get("auxilioMoradia", False)),
        "imovel_funcional": bool(rec.get("imovelFuncional", False)),
    }


def extract_all() -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    print("Fetching housing allowance snapshot...", end=" ", flush=True)

    with SenateApiClient() as client:
        data = client.get_adm("/api/v1/senadores/auxilio-moradia")

    if not data:
        print("empty — no data returned.")
        return

    if isinstance(data, dict):
        data = [data]

    records = [_flatten_record(r) for r in data if r]

    df = pl.DataFrame(records).unique(subset=["nome_parlamentar"])

    out = RAW_DIR / "auxilio_moradia.parquet"
    df.write_parquet(out)
    print(f"{len(df)} senators → {out}")


if __name__ == "__main__":
    extract_all()
