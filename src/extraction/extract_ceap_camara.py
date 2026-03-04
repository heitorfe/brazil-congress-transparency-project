"""
Extract Chamber of Deputies CEAP (Cota para o Exercício da Atividade Parlamentar)
expense reimbursements from the Câmara Federal bulk ZIP portal.

Source:
  https://www.camara.leg.br/cotas/Ano-{year}.csv.zip
  ZIP contains: Ano-{year}.csv
  Encoding: utf-8-sig (BOM-prefixed UTF-8), separator: semicolon

Coverage: 2009 → current year (~350K–600K rows/year, ~5M+ total for full history)

Output: data/raw/ceap_camara_{year}.parquet  (one Parquet per year)

Key quirks (from br-acc CamaraPipeline analysis):
  - Encoding is utf-8-sig (BOM prefix) — NOT latin-1 like the Senate CEAPS.
  - vlrLiquido is a Brazilian locale string: "1.234,56" — kept raw in Parquet.
  - nuDeputadoId (= deputado_id in dim_deputado) IS available and is the primary FK.
  - cpf field is also present in the bulk CSV (unlike the REST API per-deputy approach).
  - Stable expense_id generated here: SHA256[:16] of deputy_id|date|doc|value.

Performance note: uses Polars for vectorized CSV parsing — ~10-20x faster than
pandas iterrows() for the 350K–600K rows per year typical in this dataset.

CLI:
  python extract_ceap_camara.py                               # 2009 → current year
  python extract_ceap_camara.py --start-year 2020             # partial backfill
  python extract_ceap_camara.py --start-year 2024 --end-year 2024  # single year test
  python extract_ceap_camara.py --skip-existing               # skip already-downloaded years
"""

import hashlib
from datetime import date
from pathlib import Path

import polars as pl

from config import CEAP_CAMARA_START_YEAR, RAW_DIR
from download_utils import download_file, safe_extract_zip
from utils import configure_utf8

configure_utf8()

CEAP_BASE_URL = "https://www.camara.leg.br/cotas/Ano-{year}.csv.zip"

# Map CSV column names → our snake_case names.
# Both old (ideCadastro) and new (nuDeputadoId) deputy-ID column names are handled.
_CSV_COLUMN_MAP = {
    "txNomeParlamentar":          "nome_parlamentar",
    "cpf":                        "cpf",
    "ideCadastro":                "deputado_id",
    "nuDeputadoId":               "deputado_id",
    "sgUF":                       "uf",
    "sgPartido":                  "partido_sigla",
    "codLegislatura":             "cod_legislatura",
    "numSubCota":                 "num_sub_cota",
    "txtDescricao":               "descricao",
    "numEspecificacaoSubCota":    "num_especificacao",
    "txtDescricaoEspecificacao":  "descricao_especificacao",
    "txtFornecedor":              "fornecedor",
    "txtCNPJCPF":                 "cnpj_cpf",
    "txtNumero":                  "numero_documento",
    "indTipoDocumento":           "tipo_documento_ind",
    "datEmissao":                 "data",
    "vlrDocumento":               "valor_documento",
    "vlrGlosa":                   "valor_glosa",
    "vlrLiquido":                 "valor_liquido",
    # Older column names (pre-2019)
    "numAno":                     "ano",
    "numMes":                     "mes",
}


def _stable_hash_col(composite: pl.Series, prefix: str) -> pl.Series:
    """Apply SHA256[:16] to every element of a string Series.

    Uses map_elements (one Python call per row) but avoids the per-row
    Series object overhead of iterrows. Called once per year-file.
    """
    def _h(s: str) -> str:
        return hashlib.sha256(f"{prefix}_{s}".encode()).hexdigest()[:16]
    return composite.map_elements(_h, return_dtype=pl.Utf8)


def _classify_document(doc: str | None) -> str:
    """Return 'CPF', 'CNPJ', or 'unknown' based on digit count."""
    if not doc:
        return "unknown"
    digits = "".join(c for c in doc if c.isdigit())
    if len(digits) == 11:
        return "CPF"
    if len(digits) == 14:
        return "CNPJ"
    return "unknown"


def _download_year_zip(year: int, zip_dir: Path, *, skip_existing: bool) -> Path | None:
    """Download the bulk ZIP for a single year. Returns Path on success, None on failure."""
    dest = zip_dir / f"ceap_camara_{year}.csv.zip"
    if skip_existing and dest.exists():
        return dest

    url = CEAP_BASE_URL.format(year=year)
    print(f"  {year}: downloading...", end=" ", flush=True)
    ok = download_file(url, dest)
    if not ok:
        print(f"  {year}: download failed, skipping")
        return None
    size_mb = dest.stat().st_size / (1024 * 1024)
    print(f"done ({size_mb:.1f} MB)")
    return dest


def _extract_csv_from_zip(zip_path: Path, csv_dir: Path, year: int) -> Path | None:
    """Extract the CSV file from a Câmara bulk ZIP. Returns CSV path or None."""
    extracted = safe_extract_zip(zip_path, csv_dir)
    if not extracted:
        return None

    csv_candidates = [p for p in extracted if p.suffix.lower() == ".csv"]
    if not csv_candidates:
        expected = csv_dir / f"Ano-{year}.csv"
        if expected.exists():
            return expected
        print(f"  WARNING: No CSV found in ZIP for year {year}")
        return None

    return csv_candidates[0]


def _parse_csv(csv_path: Path, year: int) -> pl.DataFrame | None:
    """Parse a single year CSV into a Polars DataFrame.

    Uses Polars for vectorized column operations — ~10-20x faster than iterrows().
    All columns kept as strings (raw BRL values parsed later in dbt staging).
    """
    df = None
    for encoding in ("utf8-lossy", "latin1"):
        try:
            df = pl.read_csv(
                csv_path,
                encoding=encoding,
                separator=";",
                infer_schema=False,       # all columns as Utf8 strings
                null_values=["", "N/A"],
                truncate_ragged_lines=True,
            )
            break
        except Exception as e:
            if encoding == "latin1":
                print(f"  WARNING: Failed to parse {csv_path.name}: {e}")
                return None

    if df is None or df.is_empty():
        return None

    # Strip BOM from column names (utf-8-sig artifact)
    df = df.rename({c: c.strip().lstrip("\ufeff") for c in df.columns})

    # If both old and new deputy-ID columns exist, keep only nuDeputadoId
    if "ideCadastro" in df.columns and "nuDeputadoId" in df.columns:
        df = df.drop("ideCadastro")

    # Remap to snake_case
    rename_map = {k: v for k, v in _CSV_COLUMN_MAP.items() if k in df.columns}
    df = df.rename(rename_map)

    # Inject year/mes if not present in CSV (most years have them; some don't)
    if "ano" not in df.columns:
        df = df.with_columns(pl.lit(str(year)).alias("ano"))
    if "mes" not in df.columns:
        df = df.with_columns(pl.lit("").alias("mes"))

    # Fill nulls with "" for string columns so concat_str works correctly
    df = df.with_columns(
        [pl.col(c).fill_null("") for c in df.columns]
    )

    # Drop rows with empty/zero deputy ID
    if "deputado_id" not in df.columns:
        print(f"  WARNING: 'deputado_id' missing in {csv_path.name}. Cols: {df.columns}")
        return None

    df = df.filter(
        pl.col("deputado_id").str.strip_chars().is_in(["", "0"]).not_()
    )

    if df.is_empty():
        return None

    # Classify supplier document type (CPF/CNPJ) — vectorized via map_elements
    cnpj_col = "cnpj_cpf" if "cnpj_cpf" in df.columns else None
    if cnpj_col:
        df = df.with_columns(
            pl.col(cnpj_col)
            .map_elements(_classify_document, return_dtype=pl.Utf8)
            .alias("tipo_fornecedor")
        )
    else:
        df = df.with_columns(pl.lit("unknown").alias("tipo_fornecedor"))

    # Build composite key as a single string column, then hash once per row
    dep  = pl.col("deputado_id").str.strip_chars()
    dt   = pl.col("data").str.strip_chars() if "data" in df.columns else pl.lit("")
    doc  = pl.col("numero_documento").str.strip_chars() if "numero_documento" in df.columns else pl.lit("")
    val  = pl.col("valor_liquido").str.strip_chars() if "valor_liquido" in df.columns else pl.lit("")

    df = df.with_columns(
        pl.concat_str([dep, dt, doc, val], separator="|").alias("_composite")
    )
    df = df.with_columns(
        _stable_hash_col(df["_composite"], prefix="ceap_camara").alias("expense_id")
    )
    df = df.drop("_composite")

    # Reorder to canonical output column set (keep all present columns)
    output_cols = [
        "expense_id", "ano", "mes", "deputado_id", "nome_parlamentar",
        "cpf", "uf", "partido_sigla", "cod_legislatura", "num_sub_cota",
        "descricao", "descricao_especificacao", "fornecedor",
        "cnpj_cpf", "tipo_fornecedor", "numero_documento", "data",
        "valor_documento", "valor_glosa", "valor_liquido",
    ]
    return df.select([c for c in output_cols if c in df.columns])


def extract_all(
    start_year: int = CEAP_CAMARA_START_YEAR,
    end_year: int | None = None,
    *,
    skip_existing: bool = True,
) -> None:
    if end_year is None:
        end_year = date.today().year

    zip_dir = RAW_DIR / "_ceap_camara_zips"
    csv_dir = RAW_DIR / "_ceap_camara_csv"
    zip_dir.mkdir(parents=True, exist_ok=True)
    csv_dir.mkdir(parents=True, exist_ok=True)
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    print(f"\nExtracting Chamber CEAP bulk ZIP: {start_year} → {end_year}")

    for year in range(start_year, end_year + 1):
        out_parquet = RAW_DIR / f"ceap_camara_{year}.parquet"

        if skip_existing and out_parquet.exists():
            print(f"  {year}: Parquet already exists, skipping")
            continue

        zip_path = _download_year_zip(year, zip_dir, skip_existing=True)
        if zip_path is None:
            continue

        print(f"  {year}: extracting ZIP...", end=" ", flush=True)
        csv_path = _extract_csv_from_zip(zip_path, csv_dir, year)
        if csv_path is None:
            print("failed")
            continue

        print(f"parsing...", end=" ", flush=True)
        df = _parse_csv(csv_path, year)
        if df is None or df.is_empty():
            print(f"0 records, skipping")
            continue

        df = df.unique(subset=["expense_id"], keep="first")
        df = df.sort(["ano", "mes", "deputado_id"])
        df.write_parquet(out_parquet)
        print(f"{len(df):,} records → {out_parquet.name}")

    print("\nDone.")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Extract Chamber CEAP bulk ZIP (2009–present)"
    )
    parser.add_argument(
        "--start-year", type=int, default=CEAP_CAMARA_START_YEAR,
        help=f"First year to extract (default: {CEAP_CAMARA_START_YEAR})",
    )
    parser.add_argument(
        "--end-year", type=int, default=None,
        help="Last year to extract (default: current year)",
    )
    parser.add_argument(
        "--skip-existing", action="store_true", default=True,
        help="Skip years whose Parquet file already exists (default: True)",
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
