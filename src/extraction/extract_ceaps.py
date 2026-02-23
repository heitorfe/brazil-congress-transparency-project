"""
Extract CEAPS (Cota para o Exercício da Atividade Parlamentar dos Senadores) expense
reimbursements from the Brazilian Senate ADM API.

Endpoint used:
  GET /api/v1/senadores/despesas_ceaps/{ano}
  -- Returns all expense receipts for a given year (21k+ records for 2024 alone).

Strategy:
  - Fetches year-by-year from DEFAULT_START_YEAR to the current year.
  - Data is a flat JSON array (no nesting).
  - All years are concatenated and deduplicated on ``id``.
  - Output: data/raw/ceaps.parquet

Key quirk:
  - ``codSenador`` is an INTEGER in the ADM API response.
    Stored as string so it can join dim_senador.senador_id (VARCHAR).
"""

from datetime import date

from api_client import SenateApiClient
from config import RAW_DIR, DEFAULT_START_YEAR
from transforms.ceaps import flatten_ceaps_record
from utils import configure_utf8, save_parquet, unwrap_list

configure_utf8()


def extract_all(start_year: int = DEFAULT_START_YEAR, end_year: int | None = None) -> None:
    if end_year is None:
        end_year = date.today().year

    RAW_DIR.mkdir(parents=True, exist_ok=True)

    all_records: list[dict] = []

    with SenateApiClient() as client:
        for year in range(start_year, end_year + 1):
            print(f"  Fetching CEAPS for year {year}...", end=" ", flush=True)
            try:
                data = client.get_adm(f"/api/v1/senadores/despesas_ceaps/{year}")
                data = unwrap_list(data)
                if not data:
                    print("empty")
                    continue
                records = [flatten_ceaps_record(r) for r in data if r]
                all_records.extend(records)
                print(f"{len(records)} records")
            except Exception as e:
                print(f"ERROR: {e}")

    if not all_records:
        print("No CEAPS data fetched. Exiting.")
        return

    out = RAW_DIR / "ceaps.parquet"
    n = save_parquet(
        all_records,
        out,
        unique_subset=["id"],
        sort_by=["ano", "mes", "cod_senador"],
    )
    print(f"\nSaved {n} CEAPS records → {out}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Extract Senate CEAPS expense records")
    parser.add_argument(
        "--start-year",
        type=int,
        default=DEFAULT_START_YEAR,
        help=f"First year to fetch (default: {DEFAULT_START_YEAR})",
    )
    parser.add_argument(
        "--end-year",
        type=int,
        default=None,
        help="Last year to fetch (default: current year)",
    )
    args = parser.parse_args()
    extract_all(start_year=args.start_year, end_year=args.end_year)
