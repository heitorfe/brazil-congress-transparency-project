"""
Extract legislative proposals (processos) from the Brazilian Senate LEGIS API.

Endpoint used:
  GET /processo?sigla={sigla}&ano={ano}
  -- Returns proposals for a given bill type (sigla) and year.
  -- Response is a JSON array of proposal objects.

Strategy:
  - Iterates over: siglas (PL, PEC, PLP, MPV) × years (DEFAULT_START_YEAR → current year).
  - Deduplicates on ``id_processo`` across all sigla/year combinations.
  - Output: data/raw/processos.parquet

Key quirks:
  - The ``id`` field in the API response maps to ``id_processo`` in our schema.
  - ``tramitando`` arrives as "Sim" / "Não" string, not a boolean.
    Conversion happens in the dbt staging layer (tramitando = 'Sim').
  - ``dataUltimaAtualizacao`` can be absent in some records.
  - Some sigla/year combinations return an empty array — guard with ``if not data``.
"""

from datetime import date

from api_client import SenateApiClient
from config import RAW_DIR, DEFAULT_START_YEAR
from transforms.processos import flatten_processo_record
from utils import configure_utf8, save_parquet, unwrap_list

configure_utf8()

SIGLAS = ["PL", "PEC", "PLP", "MPV"]


def extract_all(start_year: int = DEFAULT_START_YEAR, end_year: int | None = None) -> None:
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
                    data = unwrap_list(data)
                    records = [flatten_processo_record(r) for r in data if r and r.get("id")]
                    all_records.extend(records)
                    print(f"  {label}  {len(records)} proposals")
                except Exception as e:
                    print(f"  {label}  ERROR: {e}")

    if not all_records:
        print("No proposal data fetched. Exiting.")
        return

    out = RAW_DIR / "processos.parquet"
    n = save_parquet(
        all_records,
        out,
        unique_subset=["id_processo"],
        sort_by=["ano_materia", "sigla_materia", "id_processo"],
    )
    print(f"\nSaved {n} legislative proposals → {out}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Extract Senate legislative proposals")
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
