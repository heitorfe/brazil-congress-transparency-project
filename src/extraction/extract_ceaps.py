"""
Extract CEAPS (Cota para o Exercício da Atividade Parlamentar dos Senadores) expense
reimbursements from the Brazilian Senate ADM API.

Endpoint used:
  GET /api/v1/senadores/despesas_ceaps/{ano}
  -- Returns all expense receipts for a given year (21k+ records for 2024 alone).

Strategy:
  - Fetches year-by-year from START_YEAR to the current year.
  - Data is a flat JSON array (no nesting).
  - All years are concatenated and deduplicated on `id`.
  - Output: data/raw/ceaps.parquet

Key quirk:
  - `codSenador` is an INTEGER in the ADM API response.
    Stored as string so it can join dim_senador.senador_id (VARCHAR).
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


def _flatten_record(rec: dict) -> dict:
    return {
        "id":                 rec.get("id"),
        "tipo_documento":     rec.get("tipoDocumento"),
        "ano":                rec.get("ano"),
        "mes":                rec.get("mes"),
        "cod_senador":        str(rec.get("codSenador") or ""),
        "nome_senador":       rec.get("nomeSenador"),
        "tipo_despesa":       rec.get("tipoDespesa"),
        "cnpj_cpf":           rec.get("cpfCnpj"),
        "fornecedor":         rec.get("fornecedor"),
        "documento":          rec.get("documento"),
        "data":               rec.get("data"),
        "detalhamento":       rec.get("detalhamento"),
        "valor_reembolsado":  rec.get("valorReembolsado"),
    }


def extract_all(start_year: int = START_YEAR, end_year: int | None = None) -> None:
    if end_year is None:
        end_year = date.today().year

    RAW_DIR.mkdir(parents=True, exist_ok=True)

    all_records: list[dict] = []

    with SenateApiClient() as client:
        for year in range(start_year, end_year + 1):
            print(f"  Fetching CEAPS for year {year}...", end=" ", flush=True)
            try:
                data = client.get_adm(f"/api/v1/senadores/despesas_ceaps/{year}")
                if not data:
                    print("empty")
                    continue
                if isinstance(data, dict):
                    data = [data]
                records = [_flatten_record(r) for r in data if r]
                all_records.extend(records)
                print(f"{len(records)} records")
            except Exception as e:
                print(f"ERROR: {e}")

    if not all_records:
        print("No CEAPS data fetched. Exiting.")
        return

    df = (
        pl.DataFrame(all_records)
        .unique(subset=["id"])
        .sort(["ano", "mes", "cod_senador"])
    )

    out = RAW_DIR / "ceaps.parquet"
    df.write_parquet(out)
    print(f"\nSaved {len(df)} CEAPS records → {out}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Extract Senate CEAPS expense records")
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
