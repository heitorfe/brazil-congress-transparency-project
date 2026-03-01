"""
Extract Chamber deputy CEAP expense records (Cota para Exercício da Atividade
Parlamentar — the Chamber equivalent of Senate CEAPS).

Endpoint used:
  GET /deputados/{id}/despesas?ano={year}&itens=100   — paginated expenses per deputy

Strategy:
  - Load the list of deputy IDs from camara_deputados_lista.parquet.
  - For each deputy × each year in range, fetch all expense pages.
  - Output: data/raw/camara_despesas.parquet

Fetch pattern: Pattern E (per-entity) nested inside a year loop.

Key differences from Senate CEAPS:
  - Values are native floats (not Brazilian-locale strings).
  - Natural dedup key is cod_documento (not a separate id field).
  - No month-level API parameter — the API returns all months for a given year.
"""

import argparse
from datetime import date
from pathlib import Path

import polars as pl

from camara_client import CamaraApiClient
from config import RAW_DIR, CAMARA_DEFAULT_START_YEAR
from transforms.camara_despesas import flatten_despesa_deputado
from utils import configure_utf8, save_parquet

configure_utf8()


def _load_deputy_ids() -> list[str]:
    """Load unique deputy IDs from the previously extracted lista parquet."""
    parquet = RAW_DIR / "camara_deputados_lista.parquet"
    if not parquet.exists():
        raise FileNotFoundError(
            f"{parquet} not found. Run extract_camara_deputados.py first."
        )
    return (
        pl.read_parquet(parquet)["deputado_id"]
        .drop_nulls()
        .unique()
        .to_list()
    )


def extract_all(
    start_year: int = CAMARA_DEFAULT_START_YEAR,
    end_year: int | None = None,
) -> None:
    if end_year is None:
        end_year = date.today().year

    RAW_DIR.mkdir(parents=True, exist_ok=True)

    deputy_ids = _load_deputy_ids()
    years = list(range(start_year, end_year + 1))
    total = len(deputy_ids) * len(years)
    print(
        f"Fetching expenses: {len(deputy_ids)} deputies × {len(years)} years"
        f" = up to {total} API calls"
    )

    all_records: list[dict] = []
    done = 0

    with CamaraApiClient() as client:
        for dep_id in deputy_ids:
            for year in years:
                done += 1
                try:
                    records = client.get_all(
                        f"/deputados/{dep_id}/despesas",
                        params={"ano": year},
                    )
                    if records:
                        rows = [flatten_despesa_deputado(dep_id, r) for r in records if r]
                        all_records.extend(rows)
                except Exception as e:
                    print(f"  [{done:>6}/{total}] deputy {dep_id} year {year} ERROR: {e}")

                if done % 500 == 0:
                    print(f"  ...{done}/{total} calls done, {len(all_records)} records so far")

    if not all_records:
        print("No expense data fetched.")
        return

    out = RAW_DIR / "camara_despesas.parquet"
    n = save_parquet(
        all_records,
        out,
        unique_subset=["cod_documento", "deputado_id"],
        sort_by=["ano", "mes", "deputado_id"],
        safe_schema=True,
    )
    print(f"\nSaved {n} expense records → {out}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Extract Chamber deputy CEAP expense records"
    )
    parser.add_argument(
        "--start-year",
        type=int,
        default=CAMARA_DEFAULT_START_YEAR,
        help=f"First year to fetch (default: {CAMARA_DEFAULT_START_YEAR})",
    )
    parser.add_argument(
        "--end-year",
        type=int,
        default=None,
        help="Last year to fetch (default: current year)",
    )
    args = parser.parse_args()
    extract_all(start_year=args.start_year, end_year=args.end_year)
