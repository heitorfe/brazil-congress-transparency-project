"""
Extract Senate administrative staff (servidores), pensioners (pensionistas),
payroll (remuneracoes), and overtime (horas-extras) from the ADM API.

Endpoints used:
  GET /api/v1/servidores/servidores              -- full staff registry snapshot
  GET /api/v1/servidores/remuneracoes/{ano}/{mes} -- monthly staff payroll
  GET /api/v1/servidores/pensionistas             -- full pensioner registry snapshot
  GET /api/v1/servidores/pensionistas/remuneracoes/{ano}/{mes} -- monthly pensioner payroll
  GET /api/v1/servidores/horas-extras/{ano}/{mes} -- monthly overtime payments

Strategy:
  - servidores and pensionistas: single full snapshot (current state only)
  - remuneracoes, pensionistas/remuneracoes, horas-extras: month-by-month loop
    from start_year-01 to the current month.
  - All monthly data is concatenated and deduplicated, then written to Parquet.
  - Nested objects (cargo, lotacao, categoria, funcao, cedido) are flattened
    to a single-level dict at extraction time.

Output files (data/raw/):
  servidores.parquet               -- ~2k rows (current snapshot)
  pensionistas.parquet             -- ~1k rows (current snapshot)
  remuneracoes_servidores.parquet  -- ~840k rows (7 years × 12 months × ~10k/month)
  remuneracoes_pensionistas.parquet -- ~200k rows
  horas_extras.parquet             -- ~50k rows (monthly summaries, no daily detail)
"""

from datetime import date

from api_client import SenateApiClient
from config import RAW_DIR, DEFAULT_START_YEAR
from transforms.servidores import (
    flatten_servidor,
    flatten_pensionista,
    flatten_remuneracao,
    flatten_remuneracao_pensionista,
    flatten_hora_extra,
)
from utils import configure_utf8, save_parquet, unwrap_list, month_windows

configure_utf8()


def extract_servidores(client: SenateApiClient) -> None:
    print("Fetching staff registry (servidores)...", flush=True)
    data = client.get_adm("/api/v1/servidores/servidores")
    data = unwrap_list(data)
    if not data:
        print("  empty — skipping")
        return

    records = [flatten_servidor(r) for r in data if r]
    out = RAW_DIR / "servidores.parquet"
    n = save_parquet(records, out, unique_subset=["sequencial"], safe_schema=True)
    print(f"  Saved {n} servidores → {out}")
    client.save_sample("servidores", data)


def extract_pensionistas(client: SenateApiClient) -> None:
    print("Fetching pensioner registry (pensionistas)...", flush=True)
    data = client.get_adm("/api/v1/servidores/pensionistas")
    data = unwrap_list(data)
    if not data:
        print("  empty — skipping")
        return

    records = [flatten_pensionista(r) for r in data if r]
    out = RAW_DIR / "pensionistas.parquet"
    n = save_parquet(records, out, unique_subset=["sequencial"], safe_schema=True)
    print(f"  Saved {n} pensionistas → {out}")
    client.save_sample("pensionistas", data)


def extract_remuneracoes(
    client: SenateApiClient, start_year: int, end_year: int
) -> None:
    print(f"Fetching staff payroll (remuneracoes) {start_year}–{end_year}...", flush=True)
    windows = month_windows(date(start_year, 1, 1), date(end_year, 12, 31))

    all_records: list[dict] = []
    for ano, mes in windows:
        print(f"  {ano}/{mes:02d}...", end=" ", flush=True)
        try:
            data = client.get_adm(f"/api/v1/servidores/remuneracoes/{ano}/{mes}")
            data = unwrap_list(data)
            if not data:
                print("empty")
                continue
            records = [flatten_remuneracao(r, ano, mes) for r in data if r]
            all_records.extend(records)
            print(f"{len(records)}")
        except Exception as e:
            print(f"ERROR: {e}")

    if not all_records:
        print("No remuneracoes data fetched.")
        return

    out = RAW_DIR / "remuneracoes_servidores.parquet"
    n = save_parquet(
        all_records,
        out,
        unique_subset=["sequencial", "ano", "mes", "tipo_folha"],
        sort_by=["ano", "mes", "sequencial"],
        safe_schema=True,
    )
    print(f"  Saved {n} remuneracoes_servidores records → {out}")


def extract_remuneracoes_pensionistas(
    client: SenateApiClient, start_year: int, end_year: int
) -> None:
    print(
        f"Fetching pensioner payroll (pensionistas/remuneracoes) {start_year}–{end_year}...",
        flush=True,
    )
    windows = month_windows(date(start_year, 1, 1), date(end_year, 12, 31))

    all_records: list[dict] = []
    for ano, mes in windows:
        print(f"  {ano}/{mes:02d}...", end=" ", flush=True)
        try:
            data = client.get_adm(
                f"/api/v1/servidores/pensionistas/remuneracoes/{ano}/{mes}"
            )
            data = unwrap_list(data)
            if not data:
                print("empty")
                continue
            records = [flatten_remuneracao_pensionista(r, ano, mes) for r in data if r]
            all_records.extend(records)
            print(f"{len(records)}")
        except Exception as e:
            print(f"ERROR: {e}")

    if not all_records:
        print("No pensionista remuneracoes data fetched.")
        return

    out = RAW_DIR / "remuneracoes_pensionistas.parquet"
    n = save_parquet(
        all_records,
        out,
        unique_subset=["sequencial", "ano", "mes"],
        sort_by=["ano", "mes", "sequencial"],
        safe_schema=True,
    )
    print(f"  Saved {n} remuneracoes_pensionistas records → {out}")


def extract_horas_extras(
    client: SenateApiClient, start_year: int, end_year: int
) -> None:
    print(f"Fetching overtime (horas-extras) {start_year}–{end_year}...", flush=True)
    windows = month_windows(date(start_year, 1, 1), date(end_year, 12, 31))

    all_records: list[dict] = []
    for ano, mes in windows:
        print(f"  {ano}/{mes:02d}...", end=" ", flush=True)
        try:
            data = client.get_adm(f"/api/v1/servidores/horas-extras/{ano}/{mes}")
            data = unwrap_list(data)
            if not data:
                print("empty")
                continue
            records = [flatten_hora_extra(r, ano, mes) for r in data if r]
            all_records.extend(records)
            print(f"{len(records)}")
        except Exception as e:
            print(f"ERROR: {e}")

    if not all_records:
        print("No horas-extras data fetched.")
        return

    out = RAW_DIR / "horas_extras.parquet"
    n = save_parquet(
        all_records,
        out,
        unique_subset=["sequencial", "ano_pagamento", "mes_pagamento"],
        sort_by=["ano_pagamento", "mes_pagamento", "sequencial"],
        safe_schema=True,
    )
    print(f"  Saved {n} horas_extras records → {out}")
    client.save_sample("horas_extras", all_records)


def extract_all(start_year: int = DEFAULT_START_YEAR, end_year: int | None = None) -> None:
    if end_year is None:
        end_year = date.today().year

    RAW_DIR.mkdir(parents=True, exist_ok=True)

    with SenateApiClient() as client:
        extract_servidores(client)
        extract_pensionistas(client)
        extract_remuneracoes(client, start_year, end_year)
        extract_remuneracoes_pensionistas(client, start_year, end_year)
        extract_horas_extras(client, start_year, end_year)

    print("\nAll servidores extractions complete.")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Extract Senate administrative staff data from ADM API"
    )
    parser.add_argument(
        "--start-year",
        type=int,
        default=DEFAULT_START_YEAR,
        help=f"First year to fetch monthly data (default: {DEFAULT_START_YEAR})",
    )
    parser.add_argument(
        "--end-year",
        type=int,
        default=None,
        help="Last year to fetch monthly data (default: current year)",
    )
    args = parser.parse_args()
    extract_all(start_year=args.start_year, end_year=args.end_year)
