"""
Extract legislative proposals (processos) from the Brazilian Senate LEGIS API.

Endpoint used:
  GET /processo?sigla={sigla}&ano={ano}
  -- Returns proposals for a given bill type (sigla) and year.
  -- Response is a JSON array of proposal objects.

Strategy:
  - Iterates over: siglas (PL, PEC, PLP, MPV) × years (2019 → current year).
  - Deduplicates on `id_processo` across all sigla/year combinations.
  - Output: data/raw/processos.parquet

Key quirks:
  - The `id` field in the API response maps to `id_processo` in our schema.
  - `tramitando` arrives as "Sim" / "Não" string, not a boolean.
    Conversion happens in the dbt staging layer (tramitando = 'Sim').
  - `dataUltimaAtualizacao` can be absent in some records.
  - Some sigla/year combinations return an empty array — guard with `if not data`.
"""

import sys
from datetime import date
from pathlib import Path

import polars as pl

from api_client import SenateApiClient

if sys.stdout.encoding != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

RAW_DIR = Path("data/raw")
START_YEAR = 2019
SIGLAS = ["PL", "PEC", "PLP", "MPV"]


def _flatten_record(rec: dict) -> dict:
    return {
        "id_processo":            rec.get("id"),
        "codigo_materia":         rec.get("codigoMateria"),
        "identificacao":          rec.get("identificacao"),
        "sigla_materia":          rec.get("identificacao", "").split(" ")[0] if rec.get("identificacao") else None,
        "numero_materia":         (rec.get("identificacao") or "").split(" ")[1].split("/")[0] if rec.get("identificacao") and " " in rec.get("identificacao", "") else None,
        "ano_materia":            int(rec.get("identificacao", "").split("/")[-1]) if rec.get("identificacao") and "/" in rec.get("identificacao", "") else None,
        "ementa":                 rec.get("ementa"),
        "tipo_documento":         rec.get("tipoDocumento"),
        "data_apresentacao":      rec.get("dataApresentacao"),
        "autoria":                rec.get("autoria"),
        "casa_identificadora":    rec.get("casaIdentificadora"),
        "tramitando":             rec.get("tramitando"),
        "data_ultima_atualizacao": rec.get("dataUltimaAtualizacao"),
        "url_documento":          rec.get("urlDocumento"),
    }


def extract_all(start_year: int = START_YEAR, end_year: int | None = None) -> None:
    if end_year is None:
        end_year = date.today().year

    RAW_DIR.mkdir(parents=True, exist_ok=True)

    all_records: list[dict] = []
    total_combos = len(SIGLAS) * (end_year - start_year + 1)
    combo_count = 0

    with SenateApiClient() as client:
        for sigla in SIGLAS:
            for year in range(start_year, end_year + 1):
                combo_count += 1
                label = f"[{combo_count:>3}/{total_combos}] {sigla}/{year}"
                try:
                    data = client.get_legis(
                        "/processo",
                        params={"sigla": sigla, "ano": year},
                        suffix="",
                    )
                    if not data:
                        print(f"  {label}  (empty)")
                        continue
                    if isinstance(data, dict):
                        # Some responses wrap in a container
                        data = data.get("processos") or data.get("Processo") or [data]
                    if not isinstance(data, list):
                        data = [data]
                    records = [_flatten_record(r) for r in data if r and r.get("id")]
                    all_records.extend(records)
                    print(f"  {label}  {len(records)} proposals")
                except Exception as e:
                    print(f"  {label}  ERROR: {e}")

    if not all_records:
        print("No proposal data fetched. Exiting.")
        return

    df = (
        pl.DataFrame(all_records)
        .unique(subset=["id_processo"])
        .sort(["ano_materia", "sigla_materia", "id_processo"])
    )

    out = RAW_DIR / "processos.parquet"
    df.write_parquet(out)
    print(f"\nSaved {len(df)} legislative proposals → {out}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Extract Senate legislative proposals")
    parser.add_argument(
        "--start-year",
        type=int,
        default=START_YEAR,
        help=f"First year to fetch (default: {START_YEAR})",
    )
    parser.add_argument(
        "--end-year",
        type=int,
        default=None,
        help="Last year to fetch (default: current year)",
    )
    args = parser.parse_args()
    extract_all(start_year=args.start_year, end_year=args.end_year)
