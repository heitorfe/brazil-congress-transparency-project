"""
Extract federal procurement contracts (compras) from the Portal da Transparência.

Source:
  https://portaldatransparencia.gov.br/download-de-dados/compras/{YYYYMM}
  ZIP contains multiple CSVs — only *_Compras.csv is loaded (skipping ItemCompra, TermoAditivo, etc.)
  Encoding: UTF-8 (Portal da Transparência default), separator: semicolon

Coverage: {start_year}-01 → current month (one Parquet per YYYYMM)

Output: data/raw/transparencia_contratos_{YYYYMM}.parquet

Key quirks:
  - Monthly ZIPs vary in the exact CSV filename; discovered dynamically by stem suffix.
  - Column names vary between months; use available_cols pattern.
  - Values in BRL locale ("1.234,56") — kept raw as strings in Parquet (parsed in dbt staging).
  - Data-entry errors exist (R$1T+ contracts) — cap at R$10B via brazil_utils.cap_value().
  - ZIP is returned as a download response (not a redirect). Include User-Agent to avoid 403.

CLI:
  python extract_transparencia.py                             # 2023-01 → current month
  python extract_transparencia.py --start-year 2023
  python extract_transparencia.py --start-year 2019 --skip-existing  # full backfill
  python extract_transparencia.py --start-year 2024 --end-year 2024  # single year
"""

import hashlib
from datetime import date
from pathlib import Path

import polars as pl

from brazil_utils import cap_value, stable_hash_id
from config import RAW_DIR, TRANSPARENCIA_CDN_BASE, TRANSPARENCIA_CONTRATOS_START_YEAR
from download_utils import download_file, safe_extract_zip, validate_csv
from utils import configure_utf8

configure_utf8()

# ── URL template ───────────────────────────────────────────────────────────────

_COMPRAS_URL = TRANSPARENCIA_CDN_BASE + "/compras/{yyyymm}"

# ── Column mapping ─────────────────────────────────────────────────────────────
# Portal da Transparência column names as of 2023-2025.
# Use available_cols pattern to handle schema drift between months.

_COMPRAS_COLS = {
    # Actual column names from Portal da Transparência compras CSVs (cp1252/latin-1 encoded)
    # Verified against 2023 monthly ZIPs — column names differ from strategy doc.
    "Código Órgão Superior":       "codigo_orgao_superior",
    "Nome Órgão Superior":         "nome_orgao_superior",
    "Código Órgão":                "codigo_orgao",
    "Nome Órgão":                  "nome_orgao",
    "Código UG":                   "codigo_ug",
    "Nome UG":                     "nome_ug",
    "Modalidade Compra":           "modalidade_compra",
    "Situação Contrato":           "situacao_contrato",
    "Fundamento Legal":            "fundamento_legal",
    "Número do Contrato":          "numero_contrato",
    "Código Contratado":           "cnpj_contratado",
    "Nome Contratado":             "nome_contratado",
    "Objeto":                      "objeto",
    "Valor Inicial Compra":        "valor_inicial_raw",
    "Valor Final Compra":          "valor_final_raw",
    "Data Assinatura Contrato":    "data_assinatura_raw",
    "Data Início Vigência":        "data_inicio_vigencia_raw",
    "Data Fim Vigência":           "data_fim_vigencia_raw",
    "Número Licitação":            "numero_licitacao",
}

# Temporary directories under data/raw/
_ZIP_DIR = RAW_DIR / "_transparencia_zips"
_CSV_DIR = RAW_DIR / "_transparencia_csvs"


# ── Internal helpers ───────────────────────────────────────────────────────────

def _months_in_range(start_year: int, end_year: int) -> list[tuple[int, int]]:
    """Return list of (year, month) tuples from start_year-01 to end_year-12 (or current month)."""
    today = date.today()
    result = []
    for y in range(start_year, end_year + 1):
        max_m = today.month if y == today.year else 12
        for m in range(1, max_m + 1):
            result.append((y, m))
    return result


def _discover_compras_csv(paths: list[Path]) -> Path | None:
    """Find the *_Compras.csv file in extracted paths (case-insensitive, ignore Item/Termo files)."""
    skip_suffixes = {"itemcompra", "termoaditivo", "itenscompra"}
    for p in paths:
        if p.suffix.lower() != ".csv":
            continue
        stem_lower = p.stem.lower()
        # Must end with "compras" and NOT be an excluded sub-file
        if stem_lower.endswith("compras") and not any(s in stem_lower for s in skip_suffixes):
            return p
    return None


def _parse_csv(csv_path: Path, yyyymm: str) -> pl.DataFrame | None:
    """Parse a single compras CSV into a Polars DataFrame with stable contract IDs."""
    df = None
    for encoding in ("latin1", "utf8-lossy", "utf8"):
        try:
            df = pl.read_csv(
                csv_path,
                encoding=encoding,
                separator=";",
                infer_schema=False,
                null_values=["", "N/A", "-"],
                truncate_ragged_lines=True,
            )
            break
        except Exception as e:
            if encoding == "utf8":
                print(f"  WARNING: Failed to parse {csv_path.name}: {e}")
                return None

    if df is None or df.is_empty():
        return None

    # Strip BOM from column names
    df = df.rename({c: c.strip().lstrip("\ufeff") for c in df.columns})

    # Map available columns only (handles schema drift between months)
    available = {k: v for k, v in _COMPRAS_COLS.items() if k in df.columns}
    if not available:
        print(f"  WARNING: No expected columns found in {csv_path.name}. Cols: {df.columns[:5]}")
        return None
    if "cnpj_contratado" not in available.values():
        print(f"  WARNING: 'Código Contratado' missing. Matched cols: {list(available.values())[:5]}")

    df = df.rename(available)

    # Add yyyymm tag
    df = df.with_columns(pl.lit(yyyymm).alias("yyyymm"))

    # Ensure mandatory columns exist (fill with empty string for hash stability)
    for col in ["codigo_ug", "numero_contrato", "cnpj_contratado",
                "data_assinatura_raw", "valor_inicial_raw", "nome_orgao"]:
        if col not in df.columns:
            df = df.with_columns(pl.lit(None, dtype=pl.Utf8).alias(col))

    df = df.with_columns(
        [pl.col(c).fill_null("") for c in df.columns]
    )

    # Stable contract ID (no numero_processo in actual CSV)
    key = pl.concat_str(
        [pl.col("codigo_ug"), pl.col("numero_contrato"),
         pl.col("cnpj_contratado"), pl.col("data_assinatura_raw")],
        separator="|",
    )
    df = df.with_columns(key.alias("_key"))

    def _hash(s: str) -> str:
        return hashlib.sha256(f"transp_contrato_{s}".encode()).hexdigest()[:16]

    df = df.with_columns(
        pl.col("_key").map_elements(_hash, return_dtype=pl.Utf8).alias("contrato_id")
    ).drop("_key")

    # Cap R$10B data-entry errors — parse float, cap, set to None if over limit
    def _cap_raw(raw: str | None) -> str | None:
        if not raw:
            return raw
        cleaned = raw.replace(".", "").replace(",", ".")
        try:
            capped = cap_value(float(cleaned))
            return raw if capped is not None else None
        except ValueError:
            return raw

    if "valor_inicial_raw" in df.columns:
        df = df.with_columns(
            pl.col("valor_inicial_raw").map_elements(_cap_raw, return_dtype=pl.Utf8)
        )

    # Derive ano_compra from data_assinatura_raw (format: DD/MM/YYYY → last 4 chars)
    if "data_assinatura_raw" in df.columns:
        df = df.with_columns(
            pl.col("data_assinatura_raw")
            .str.slice(-4)
            .alias("ano_compra")
        )

    # Canonical output columns
    _OUTPUT_COLS = [
        "contrato_id", "yyyymm", "ano_compra",
        "codigo_orgao_superior", "nome_orgao_superior",
        "codigo_orgao", "nome_orgao",
        "codigo_ug", "nome_ug",
        "modalidade_compra", "situacao_contrato", "numero_contrato",
        "cnpj_contratado", "nome_contratado", "objeto",
        "valor_inicial_raw", "valor_final_raw",
        "data_assinatura_raw", "data_inicio_vigencia_raw", "data_fim_vigencia_raw",
        "numero_licitacao",
    ]
    for col in _OUTPUT_COLS:
        if col not in df.columns:
            df = df.with_columns(pl.lit(None, dtype=pl.Utf8).alias(col))

    return df.select(_OUTPUT_COLS)


def _print_stats(df: pl.DataFrame, yyyymm: str) -> None:
    total = len(df)
    null_cnpj = df.filter(pl.col("cnpj_contratado").is_null() | (pl.col("cnpj_contratado") == "")).height
    has_value = df.filter(pl.col("valor_inicial_raw").is_not_null() & (pl.col("valor_inicial_raw") != "")).height
    print(f"  {yyyymm}: {total:,} rows | null CNPJ: {null_cnpj:,} ({null_cnpj/total*100:.1f}%) | has value: {has_value:,}")

    if "nome_orgao" in df.columns:
        top_organs = (
            df.filter(pl.col("nome_orgao").is_not_null() & (pl.col("nome_orgao") != ""))
            .group_by("nome_orgao")
            .agg(pl.len().alias("n"))
            .sort("n", descending=True)
            .head(3)
        )
        organs_str = ", ".join(f"{r['nome_orgao']}={r['n']:,}" for r in top_organs.to_dicts())
        print(f"           top organs: {organs_str}")


# ── Main extraction ────────────────────────────────────────────────────────────

def extract_all(
    start_year: int = TRANSPARENCIA_CONTRATOS_START_YEAR,
    end_year: int | None = None,
    *,
    skip_existing: bool = True,
) -> None:
    """Download and save contract parquets for all months in the given year range."""
    if end_year is None:
        end_year = date.today().year

    _ZIP_DIR.mkdir(parents=True, exist_ok=True)
    _CSV_DIR.mkdir(parents=True, exist_ok=True)
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    months = _months_in_range(start_year, end_year)
    print(f"\nExtracting Portal da Transparência compras: {start_year}-01 → {end_year}-{months[-1][1]:02d} ({len(months)} months)")

    for year, month in months:
        yyyymm = f"{year}{month:02d}"
        out_parquet = RAW_DIR / f"transparencia_contratos_{yyyymm}.parquet"

        if skip_existing and out_parquet.exists():
            print(f"  {yyyymm}: already exists, skipping")
            continue

        url = _COMPRAS_URL.format(yyyymm=yyyymm)
        zip_dest = _ZIP_DIR / f"compras_{yyyymm}.zip"

        print(f"  {yyyymm}: downloading...", end=" ", flush=True)
        ok = download_file(url, zip_dest)
        if not ok:
            print(f"download failed, skipping")
            continue
        size_mb = zip_dest.stat().st_size / (1024 * 1024)
        print(f"done ({size_mb:.1f} MB), extracting...", end=" ", flush=True)

        extracted_paths = safe_extract_zip(zip_dest, _CSV_DIR)
        if not extracted_paths:
            print("no files extracted, skipping")
            continue

        csv_path = _discover_compras_csv(extracted_paths)
        if csv_path is None:
            all_names = [p.name for p in extracted_paths]
            print(f"no *_Compras.csv found in ZIP. Files: {all_names[:5]}, skipping")
            continue

        if not validate_csv(csv_path, encoding="latin-1", sep=";"):
            print(f"CSV validation failed for {csv_path.name}, skipping")
            continue

        print(f"parsing...", end=" ", flush=True)
        df = _parse_csv(csv_path, yyyymm)
        if df is None or df.is_empty():
            print("0 records, skipping")
            continue

        df = df.unique(subset=["contrato_id"], keep="first")
        df.write_parquet(out_parquet)
        print(f"{len(df):,} contracts → {out_parquet.name}")
        _print_stats(df, yyyymm)

    print("\nDone.")


# ── CLI ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Extract Portal da Transparência compras (monthly ZIPs)"
    )
    parser.add_argument(
        "--start-year", type=int, default=TRANSPARENCIA_CONTRATOS_START_YEAR,
        help=f"First year to extract (default: {TRANSPARENCIA_CONTRATOS_START_YEAR})",
    )
    parser.add_argument(
        "--end-year", type=int, default=None,
        help="Last year to extract (default: current year)",
    )
    parser.add_argument(
        "--skip-existing", action="store_true", default=True,
        help="Skip months whose Parquet already exists (default: True)",
    )
    parser.add_argument(
        "--no-skip-existing", dest="skip_existing", action="store_false",
        help="Re-download and overwrite existing Parquet files",
    )
    args = parser.parse_args()
    extract_all(
        start_year=args.start_year,
        end_year=args.end_year,
        skip_existing=args.skip_existing,
    )
