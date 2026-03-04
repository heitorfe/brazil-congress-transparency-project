"""
Extract Senate CEAPS (Cota para o Exercício da Atividade Parlamentar dos Senadores)
expense reimbursements from the Senado Federal bulk CSV portal.

Source:
  https://www.senado.leg.br/transparencia/LAI/verba/despesa_ceaps_{year}.csv
  Encoding: latin-1, separator: semicolon

Coverage: 2008 → current year (~21K rows/year, ~300K total for full history)

Output: data/raw/ceaps_senado_{year}.parquet  (one Parquet per year)

Key quirks (from br-acc SenadoPipeline analysis):
  - Row 0 is a metadata line ("ULTIMA ATUALIZACAO";"<timestamp>") — skip it.
  - Values in Brazilian locale: "1.234,56" — kept as raw strings in Parquet,
    parsed in dbt staging with REPLACE/REPLACE/TRY_CAST.
  - CNPJ_CPF contains both CNPJ (14 digits) and CPF (11 digits).
  - SENADOR field is the parliamentary name (no numeric ID in bulk CSV).
    Join to dim_senador happens via name matching in dbt.
  - Stable expense_id generated here for deduplication: SHA256[:16] composite key.

CLI:
  python extract_ceaps_senado.py                              # 2008 → current year
  python extract_ceaps_senado.py --start-year 2020            # partial backfill
  python extract_ceaps_senado.py --start-year 2024 --end-year 2024  # single year test
  python extract_ceaps_senado.py --skip-existing              # skip already-downloaded years
"""

from datetime import date
from pathlib import Path

import pandas as pd

from brazil_utils import classify_document, stable_hash_id
from config import CEAPS_BULK_START_YEAR, RAW_DIR
from download_utils import download_file
from utils import configure_utf8, save_parquet

configure_utf8()

CEAPS_BASE_URL = "https://www.senado.leg.br/transparencia/LAI/verba/despesa_ceaps_{year}.csv"

# Column names as they appear in the Senate bulk CSV (all uppercase)
_CSV_COLUMNS = [
    "ANO", "MES", "SENADOR", "TIPO_DESPESA", "CNPJ_CPF",
    "FORNECEDOR", "DOCUMENTO", "DATA", "DETALHAMENTO", "VALOR_REEMBOLSADO",
]


def _download_year_csv(year: int, raw_csv_dir: Path, *, skip_existing: bool) -> Path | None:
    """Download the bulk CSV for a single year. Returns Path on success, None on failure."""
    dest = raw_csv_dir / f"ceaps_senado_{year}.csv"
    if skip_existing and dest.exists():
        print(f"  {year}: already downloaded, skipping")
        return dest

    url = CEAPS_BASE_URL.format(year=year)
    print(f"  {year}: downloading...", end=" ", flush=True)
    ok = download_file(url, dest)
    if not ok:
        print(f"  {year}: download failed, skipping")
        return None
    print(f"done ({dest.stat().st_size // 1024} KB)")
    return dest


def _parse_csv(csv_path: Path, year: int) -> list[dict]:
    """Parse a single year CSV into a list of flat dicts.

    Row 0 is always the metadata line ("ULTIMA ATUALIZACAO";"<timestamp>").
    Row 1 is the actual header. skiprows=1 skips the metadata row.
    """
    try:
        df = pd.read_csv(
            csv_path,
            encoding="latin-1",
            sep=";",
            dtype=str,
            skiprows=1,  # skip the "ULTIMA ATUALIZACAO" metadata line
            on_bad_lines="skip",
        )
    except Exception as e:
        print(f"  WARNING: Failed to parse {csv_path.name}: {e}")
        return []

    # Normalize column names (strip quotes/whitespace, uppercase)
    df.columns = [c.strip().strip('"').strip().upper() for c in df.columns]

    # Keep only known columns (guard against years with extra/renamed columns)
    available = [c for c in _CSV_COLUMNS if c in df.columns]
    if len(available) < 5:
        print(f"  WARNING: {csv_path.name} has unexpected columns: {list(df.columns)}")
        return []

    df = df[available].copy()
    df.fillna("", inplace=True)

    records = []
    for _, row in df.iterrows():
        senador = str(row.get("SENADOR", "")).strip().strip('"')
        documento = str(row.get("DOCUMENTO", "")).strip()
        valor_raw = str(row.get("VALOR_REEMBOLSADO", "")).strip()
        data_str = str(row.get("DATA", "")).strip()
        mes = str(row.get("MES", "")).strip()
        cnpj_cpf = str(row.get("CNPJ_CPF", "")).strip()

        # Skip rows with no senator name (header duplicates, empty rows)
        if not senador or senador.upper() in ("SENADOR", ""):
            continue

        # Stable expense ID: composite key hashed
        composite = f"{senador}|{year}|{mes}|{data_str}|{documento}|{valor_raw}"
        expense_id = stable_hash_id(composite, prefix="ceaps_senado")

        records.append({
            "expense_id":        expense_id,
            "ano":               year,
            "mes":               mes,
            "senador_nome":      senador,
            "tipo_despesa":      str(row.get("TIPO_DESPESA", "")).strip(),
            "cnpj_cpf":          cnpj_cpf,
            "tipo_documento":    classify_document(cnpj_cpf),
            "fornecedor":        str(row.get("FORNECEDOR", "")).strip(),
            "documento":         documento,
            "data":              data_str,
            "detalhamento":      str(row.get("DETALHAMENTO", "")).strip(),
            # Raw BRL string kept as-is; parsed in dbt staging
            "valor_reembolsado": valor_raw,
        })

    return records


def extract_all(
    start_year: int = CEAPS_BULK_START_YEAR,
    end_year: int | None = None,
    *,
    skip_existing: bool = True,
) -> None:
    if end_year is None:
        end_year = date.today().year

    raw_csv_dir = RAW_DIR / "_ceaps_senado_csv"
    raw_csv_dir.mkdir(parents=True, exist_ok=True)
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    print(f"\nExtracting Senate CEAPS bulk CSV: {start_year} → {end_year}")

    for year in range(start_year, end_year + 1):
        out_parquet = RAW_DIR / f"ceaps_senado_{year}.parquet"

        if skip_existing and out_parquet.exists():
            print(f"  {year}: Parquet already exists, skipping")
            continue

        csv_path = _download_year_csv(year, raw_csv_dir, skip_existing=True)
        if csv_path is None:
            continue

        records = _parse_csv(csv_path, year)
        if not records:
            print(f"  {year}: 0 records parsed, skipping Parquet write")
            continue

        n = save_parquet(
            records,
            out_parquet,
            unique_subset=["expense_id"],
            sort_by=["ano", "mes", "senador_nome"],
        )
        print(f"  {year}: {n:,} records → {out_parquet.name}")

    print("\nDone.")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Extract Senate CEAPS bulk CSV (2008–present)"
    )
    parser.add_argument(
        "--start-year",
        type=int,
        default=CEAPS_BULK_START_YEAR,
        help=f"First year to extract (default: {CEAPS_BULK_START_YEAR})",
    )
    parser.add_argument(
        "--end-year",
        type=int,
        default=None,
        help="Last year to extract (default: current year)",
    )
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        default=True,
        help="Skip years whose Parquet file already exists (default: True)",
    )
    parser.add_argument(
        "--no-skip-existing",
        dest="skip_existing",
        action="store_false",
        help="Re-download and overwrite existing Parquet files",
    )
    args = parser.parse_args()
    extract_all(
        start_year=args.start_year,
        end_year=args.end_year,
        skip_existing=args.skip_existing,
    )
