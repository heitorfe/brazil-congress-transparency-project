"""
Extract legislative proposals authored by Chamber deputies.

Endpoint used:
  GET /proposicoes?idDeputadoAutor={id}&ano={year}&itens=100

Strategy:
  - Load the list of deputy IDs from camara_deputados_lista.parquet.
  - For each deputy, iterate over each year in [start_year, end_year] and fetch
    proposals with the `ano` year filter. This avoids fetching thousands of
    historical proposals from long-serving deputies who first entered the Chamber
    before 2019.
    NOTE: The Chamber API rejects `dataApresentacaoInicio/Fim` date range filters
    when combined with `idDeputadoAutor` (returns 400), but the `ano` integer
    year filter works correctly.
  - Paginate via "next" links until all pages are consumed.
  - Output: data/raw/camara_proposicoes.parquet

Fetch pattern: Pattern E (per-entity × per-year), full pagination.

Note: A proposal may be co-authored by multiple deputies. This extractor
captures the (proposicao, deputado) pair as returned by the API search.
Deduplication on (proposicao_id, deputado_id_autor) ensures each pair is unique.
"""

import argparse
from datetime import date

import polars as pl

from camara_client import CamaraApiClient
from config import RAW_DIR, CAMARA_DEFAULT_START_YEAR
from transforms.camara_proposicoes import flatten_proposicao
from utils import configure_utf8, save_parquet

configure_utf8()


def _load_deputy_ids() -> list[str]:
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
    """
    Parameters
    ----------
    start_year : int
        First year of proposals to fetch (uses the `ano` API filter).
        Defaults to CAMARA_DEFAULT_START_YEAR (2019).
    end_year : int | None
        Last year of proposals to fetch.
        Defaults to the current year.
    """
    if end_year is None:
        end_year = date.today().year

    RAW_DIR.mkdir(parents=True, exist_ok=True)

    deputy_ids = _load_deputy_ids()
    years = list(range(start_year, end_year + 1))
    print(
        f"Fetching proposals for {len(deputy_ids)} deputies"
        f" × {len(years)} years ({start_year}–{end_year})..."
    )

    all_records: list[dict] = []
    total_combinations = len(deputy_ids) * len(years)
    done = 0

    with CamaraApiClient() as client:
        for i, dep_id in enumerate(deputy_ids, 1):
            for year in years:
                try:
                    records = client.get_all(
                        "/proposicoes",
                        params={"idDeputadoAutor": dep_id, "ano": year},
                    )
                    if records:
                        rows = [flatten_proposicao(r, dep_id) for r in records if r]
                        all_records.extend(rows)
                except Exception as e:
                    print(f"  deputy {dep_id} year {year} ERROR: {e}")

                done += 1

            if i % 100 == 0 or i == len(deputy_ids):
                print(
                    f"  ...{i}/{len(deputy_ids)} deputies done"
                    f" ({done}/{total_combinations} year-slices),"
                    f" {len(all_records)} proposals so far"
                )

    if not all_records:
        print("No proposal data fetched.")
        return

    print(f"\n{len(all_records)} proposals fetched for {start_year}–{end_year}")

    out = RAW_DIR / "camara_proposicoes.parquet"
    n = save_parquet(
        all_records,
        out,
        unique_subset=["proposicao_id", "deputado_id_autor"],
        sort_by=["ano", "proposicao_id"],
        safe_schema=True,
    )
    print(f"Saved {n} proposal records → {out}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Extract Chamber deputy legislative proposals"
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
