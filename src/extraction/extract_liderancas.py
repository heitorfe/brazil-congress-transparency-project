"""
Extract current leadership positions from the Brazilian Senate LEGIS API.

Endpoint used:
  GET /composicao/lideranca.json
  -- Returns a flat JSON array of 314 leadership records.
  -- Each record identifies a senator/deputy in a government, party, or bloc
     leadership role (Líder or Vice-Líder).

Strategy:
  - Single API call — no pagination or date windowing needed.
  - Output: data/raw/liderancas.parquet

Key quirks:
  - `codigoParlamentar` is an integer — stored as string for FK join to dim_senador.
  - Not all records have a `codigoPartido` (leadership-specific party) — government
    leaders use `codigoPartidoFiliacao` (the senator's own party affiliation).
  - `numeroOrdemViceLider` is optional (null for primary leaders).
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
        "codigo":                       rec.get("codigo"),
        "casa":                         rec.get("casa"),
        "sigla_tipo_unidade_lideranca": rec.get("siglaTipoUnidadeLideranca"),
        "descricao_tipo_unidade":       rec.get("descricaoTipoUnidadeLideranca"),
        "codigo_parlamentar":           str(rec.get("codigoParlamentar") or ""),
        "nome_parlamentar":             rec.get("nomeParlamentar"),
        "data_designacao":              rec.get("dataDesignacao"),
        "sigla_tipo_lideranca":         rec.get("siglaTipoLideranca"),
        "descricao_tipo_lideranca":     rec.get("descricaoTipoLideranca"),
        "numero_ordem_vice_lider":      rec.get("numeroOrdemViceLider"),
        # Party-specific leadership (optional — only for party/bloc leaders)
        "codigo_partido":               str(rec.get("codigoPartido") or ""),
        "sigla_partido":                rec.get("siglaPartido"),
        "nome_partido":                 rec.get("nomePartido"),
        # Senator's own party affiliation
        "codigo_partido_filiacao":      str(rec.get("codigoPartidoFiliacao") or ""),
        "sigla_partido_filiacao":       rec.get("siglaPartidoFiliacao"),
        "nome_partido_filiacao":        rec.get("nomePartidoFiliacao"),
    }


def extract_all() -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    print("Fetching leadership positions from /composicao/lideranca...", end=" ", flush=True)

    with SenateApiClient() as client:
        data = client.get_legis("/composicao/lideranca")

    if not data:
        print("empty — no data returned.")
        return

    # Response is a flat JSON array
    if isinstance(data, dict):
        data = [data]

    records = [_flatten_record(r) for r in data if r and r.get("codigo")]

    df = pl.DataFrame(records).unique(subset=["codigo"])

    out = RAW_DIR / "liderancas.parquet"
    df.write_parquet(out)
    print(f"{len(df)} leadership records → {out}")


if __name__ == "__main__":
    extract_all()
