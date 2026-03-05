"""
Extract TransfereGov parliamentary amendment recipients from the Portal da Transparência.

Source:
  https://portaldatransparencia.gov.br/download-de-dados/emendas-parlamentares/{year}
  Each yearly ZIP contains 3 CSV files:
    - *EmendasParlamentares.csv        (amendment metadata; 1 row per emenda)
    - *EmendasParlamentares_PorFavorecido.csv  (recipients; 1 row per emenda × CNPJ/CPF)
    - *EmendasParlamentares_Convenios.csv      (convenios; 1 row per convenio)
  Encoding: UTF-8 (try first) or latin-1; separator: semicolon

Coverage: 2015 → current year (one set of 3 Parquets per year)

Output:
  data/raw/transferegov_emendas_{year}.parquet
  data/raw/transferegov_favorecidos_{year}.parquet
  data/raw/transferegov_convenios_{year}.parquet

Key notes:
  - The favorecidos file is the KEY new data: explicit CNPJ/CPF of amendment recipients.
    The existing emendas pipeline (fct_emenda_documento) has favorecido name strings but
    not structured CNPJ. This enables "follow the money" cross-references.
  - codigo_emenda matches dim_emenda.codigo_emenda (12-char SIAFI code).
  - File stems inside ZIP discovered dynamically (filenames vary by year).
  - Column names vary slightly; always use available_cols pattern.

CLI:
  python extract_transferegov.py                                 # 2015 → current year
  python extract_transferegov.py --start-year 2020
  python extract_transferegov.py --start-year 2015 --skip-existing
  python extract_transferegov.py --start-year 2024 --end-year 2024
"""

import hashlib
from datetime import date
from pathlib import Path

import polars as pl

from config import RAW_DIR, TRANSFEREGOV_CDN_BASE, TRANSFEREGOV_START_YEAR
from download_utils import download_file, safe_extract_zip, validate_csv
from utils import configure_utf8

configure_utf8()

# ── URL template ───────────────────────────────────────────────────────────────

_EMENDAS_URL = TRANSFEREGOV_CDN_BASE + "/emendas-parlamentares/{year}"

# ── Temporary directories ──────────────────────────────────────────────────────

_ZIP_DIR = RAW_DIR / "_transferegov_zips"
_CSV_DIR = RAW_DIR / "_transferegov_csvs"

# ── Column mappings ────────────────────────────────────────────────────────────

_EMENDAS_COLS = {
    "Código da Emenda":                     "codigo_emenda",
    "Número da Emenda":                     "numero_emenda",
    "Tipo de Emenda":                       "tipo_emenda",
    "Código do Autor":                      "codigo_autor_emenda",
    "Nome do Autor":                        "nome_autor_emenda",
    "Número do CNPJ do Autor":              "cnpj_autor",
    "Localidade do gasto":                  "localidade_gasto",
    "UF":                                   "uf",
    "Função":                               "funcao",
    "Subfunção":                            "subfuncao",
    "Dotação Inicial":                      "dotacao_inicial_raw",
    "Dotação Atualizada":                   "dotacao_atualizada_raw",
    "Empenhado":                            "valor_empenhado_raw",
    "Liquidado":                            "valor_liquidado_raw",
    "Pago":                                 "valor_pago_raw",
    "Restos a Pagar Inscritos":             "restos_inscrito_raw",
    "Restos a Pagar Cancelados":            "restos_cancelado_raw",
    "Restos a Pagar Pagos":                 "restos_pagos_raw",
}

_FAVORECIDOS_COLS = {
    # Verified against 2025 consolidated snapshot (800K rows)
    "Código da Emenda":             "codigo_emenda",
    "Código do Autor da Emenda":    "codigo_autor_emenda",
    "Nome do Autor da Emenda":      "nome_autor_emenda",
    "Número da emenda":             "numero_emenda",
    "Tipo de Emenda":               "tipo_emenda",
    "Ano/Mês":                      "ano_mes_raw",          # YYYYMM → derive ano
    "Código do Favorecido":         "codigo_favorecido",
    "Favorecido":                   "nome_favorecido",
    "Natureza Jurídica":            "natureza_juridica",
    "Tipo Favorecido":              "tipo_pessoa",           # 'Pessoa Jurídica' / 'Pessoa Física'
    "UF Favorecido":                "uf_favorecido",
    "Município Favorecido":         "municipio_favorecido",
    "Valor Recebido":               "valor_transferido_raw",
    # Alternative column names seen in older years
    "Valor Transferido":            "valor_transferido_raw",
    "Valor do Repasse":             "valor_transferido_raw",
    "Município":                    "municipio_favorecido",
    "UF":                           "uf_favorecido",
    "Tipo de Pessoa":               "tipo_pessoa",
    "Valor Empenhado":              "valor_empenhado_raw",
    "Valor Pago":                   "valor_pago_raw",
}

_CONVENIOS_COLS = {
    "Código da Emenda":             "codigo_emenda",
    "Código do Convênio/Contrato":  "codigo_convenio",
    "Modalidade":                   "modalidade",
    "Situação":                     "situacao",
    "Objeto":                       "objeto",
    "Código do Proponente":         "codigo_proponente",
    "Proponente":                   "nome_proponente",
    "Município do Proponente":      "municipio_proponente",
    "UF do Proponente":             "uf_proponente",
    "Valor Global":                 "valor_global_raw",
    "Valor Liberado":               "valor_liberado_raw",
}


# ── File discovery ─────────────────────────────────────────────────────────────

def _classify_csv_file(p: Path) -> str | None:
    """Return 'emendas', 'favorecidos', or 'convenios' based on file stem."""
    if p.suffix.lower() != ".csv":
        return None
    stem_lower = p.stem.lower().replace("-", "").replace("_", "")
    if "porfavorecido" in stem_lower or "favorecido" in stem_lower:
        return "favorecidos"
    if "convenio" in stem_lower:
        return "convenios"
    if "emendasparlamentares" in stem_lower or "emendas" in stem_lower:
        return "emendas"
    return None


def _discover_files(paths: list[Path]) -> dict[str, Path | None]:
    """Map 'emendas', 'favorecidos', 'convenios' → Path (or None if not found)."""
    result: dict[str, Path | None] = {"emendas": None, "favorecidos": None, "convenios": None}
    for p in paths:
        kind = _classify_csv_file(p)
        if kind and result[kind] is None:
            result[kind] = p
    return result


# ── CSV parsers ────────────────────────────────────────────────────────────────

def _read_csv(csv_path: Path) -> pl.DataFrame | None:
    """Read a CSV with UTF-8 → latin-1 fallback; return None on failure.

    NOTE: try latin1 before utf8-lossy — the Portal da Transparência CSVs
    are latin-1. utf8-lossy replaces invalid bytes with U+FFFD, which corrupts
    column names (e.g. 'C\ufffdigo' ≠ 'Código') and breaks column mapping.
    """
    for encoding in ("utf8", "latin1", "utf8-lossy"):
        try:
            df = pl.read_csv(
                csv_path,
                encoding=encoding,
                separator=";",
                infer_schema=False,
                null_values=["", "N/A", "-"],
                truncate_ragged_lines=True,
            )
            # Strip BOM from column names
            df = df.rename({c: c.strip().lstrip("\ufeff") for c in df.columns})
            return df
        except Exception as e:
            if encoding == "utf8-lossy":
                print(f"  WARNING: Cannot parse {csv_path.name}: {e}")
                return None
    return None


def _parse_emendas(csv_path: Path, year: int) -> pl.DataFrame | None:
    df = _read_csv(csv_path)
    if df is None or df.is_empty():
        return None
    available = {k: v for k, v in _EMENDAS_COLS.items() if k in df.columns}
    df = df.rename(available)
    df = df.with_columns(pl.lit(str(year)).alias("ano"))
    for col in ["codigo_emenda", "tipo_emenda", "nome_autor_emenda", "valor_pago_raw"]:
        if col not in df.columns:
            df = df.with_columns(pl.lit(None, dtype=pl.Utf8).alias(col))
    return df


def _parse_favorecidos(csv_path: Path, year: int) -> pl.DataFrame | None:
    df = _read_csv(csv_path)
    if df is None or df.is_empty():
        return None

    available = {k: v for k, v in _FAVORECIDOS_COLS.items() if k in df.columns}
    df = df.rename(available)

    # Derive ano from Ano/Mês (YYYYMM) when available — portal returns consolidated file
    # so the loop year doesn't reflect actual record year.
    if "ano_mes_raw" in df.columns:
        df = df.with_columns(
            pl.col("ano_mes_raw").str.slice(0, 4).alias("ano")
        )
    else:
        df = df.with_columns(pl.lit(str(year)).alias("ano"))

    for col in [
        "codigo_emenda", "codigo_favorecido", "nome_favorecido", "tipo_pessoa",
        "municipio_favorecido", "uf_favorecido", "natureza_juridica",
        "codigo_autor_emenda", "nome_autor_emenda", "numero_emenda", "tipo_emenda",
        "valor_transferido_raw", "valor_empenhado_raw", "valor_pago_raw",
    ]:
        if col not in df.columns:
            df = df.with_columns(pl.lit(None, dtype=pl.Utf8).alias(col))

    df = df.with_columns(
        [pl.col(c).fill_null("") for c in df.columns]
    )

    # Filter out rows with no emenda or no favorecido
    df = df.filter(
        pl.col("codigo_emenda").str.strip_chars().ne("") &
        pl.col("codigo_favorecido").str.strip_chars().ne("")
    )
    if df.is_empty():
        return None

    # Stable favorecido ID — keyed on emenda + ano/mes + favorecido to be unique per transfer
    key_col = "ano_mes_raw" if "ano_mes_raw" in df.columns else "ano"
    key = pl.concat_str(
        [pl.col("codigo_emenda"), pl.col(key_col), pl.col("codigo_favorecido")],
        separator="|",
    )
    df = df.with_columns(key.alias("_key"))

    def _hash(s: str) -> str:
        return hashlib.sha256(f"transp_favorecido_{s}".encode()).hexdigest()[:16]

    df = df.with_columns(
        pl.col("_key").map_elements(_hash, return_dtype=pl.Utf8).alias("favorecido_id")
    ).drop("_key")

    _OUTPUT = [
        "favorecido_id", "codigo_emenda", "ano",
        "codigo_autor_emenda", "nome_autor_emenda", "numero_emenda", "tipo_emenda",
        "codigo_favorecido", "nome_favorecido", "natureza_juridica", "tipo_pessoa",
        "municipio_favorecido", "uf_favorecido",
        "valor_transferido_raw", "valor_empenhado_raw", "valor_pago_raw",
    ]
    for col in _OUTPUT:
        if col not in df.columns:
            df = df.with_columns(pl.lit(None, dtype=pl.Utf8).alias(col))
    return df.select(_OUTPUT)


def _parse_convenios(csv_path: Path, year: int) -> pl.DataFrame | None:
    df = _read_csv(csv_path)
    if df is None or df.is_empty():
        return None
    available = {k: v for k, v in _CONVENIOS_COLS.items() if k in df.columns}
    df = df.rename(available)
    df = df.with_columns(pl.lit(str(year)).alias("ano"))
    if "codigo_emenda" not in df.columns:
        df = df.with_columns(pl.lit(None, dtype=pl.Utf8).alias("codigo_emenda"))
    return df


# ── Validation stats ───────────────────────────────────────────────────────────

def _print_stats(df_fav: pl.DataFrame, year: int) -> None:
    if df_fav is None or df_fav.is_empty():
        return
    total = len(df_fav)
    pj = df_fav.filter(pl.col("tipo_pessoa").str.contains("(?i)jur")).height
    pf = df_fav.filter(pl.col("tipo_pessoa").str.contains("(?i)f[íi]s")).height
    print(f"  {year}: favorecidos={total:,} | PJ={pj:,} ({pj/total*100:.1f}%) | PF={pf:,} ({pf/total*100:.1f}%)")


# ── Main extraction ────────────────────────────────────────────────────────────

def extract_all(
    start_year: int = TRANSFEREGOV_START_YEAR,
    end_year: int | None = None,
    *,
    skip_existing: bool = True,
) -> None:
    """Download and save TransfereGov amendment parquets for the given year range."""
    if end_year is None:
        end_year = date.today().year

    _ZIP_DIR.mkdir(parents=True, exist_ok=True)
    _CSV_DIR.mkdir(parents=True, exist_ok=True)
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    print(f"\nExtracting TransfereGov emendas-parlamentares: {start_year} → {end_year}")

    for year in range(start_year, end_year + 1):
        fav_parquet = RAW_DIR / f"transferegov_favorecidos_{year}.parquet"
        eme_parquet = RAW_DIR / f"transferegov_emendas_{year}.parquet"
        conv_parquet = RAW_DIR / f"transferegov_convenios_{year}.parquet"

        if skip_existing and fav_parquet.exists() and eme_parquet.exists():
            print(f"  {year}: Parquets already exist, skipping")
            continue

        url = _EMENDAS_URL.format(year=year)
        zip_dest = _ZIP_DIR / f"emendas_parlamentares_{year}.zip"

        print(f"  {year}: downloading...", end=" ", flush=True)
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

        files = _discover_files(extracted_paths)
        all_names = [p.name for p in extracted_paths if p.suffix.lower() == ".csv"]
        print(f"found CSVs: {all_names}")

        # ── Favorecidos (main new data) ─────────────────────────────────────────
        if files["favorecidos"]:
            if not skip_existing or not fav_parquet.exists():
                df_fav = _parse_favorecidos(files["favorecidos"], year)
                if df_fav is not None and not df_fav.is_empty():
                    df_fav = df_fav.unique(subset=["favorecido_id"], keep="first")
                    df_fav.write_parquet(fav_parquet)
                    print(f"  {year}: {len(df_fav):,} favorecidos → {fav_parquet.name}")
                    _print_stats(df_fav, year)
                else:
                    print(f"  {year}: favorecidos parse yielded 0 records")
        else:
            print(f"  {year}: WARNING — no favorecidos CSV found in ZIP")

        # ── Emendas metadata ───────────────────────────────────────────────────
        if files["emendas"]:
            if not skip_existing or not eme_parquet.exists():
                df_eme = _parse_emendas(files["emendas"], year)
                if df_eme is not None and not df_eme.is_empty():
                    df_eme.write_parquet(eme_parquet)
                    print(f"  {year}: {len(df_eme):,} emendas metadata → {eme_parquet.name}")

        # ── Convênios ──────────────────────────────────────────────────────────
        if files["convenios"]:
            if not skip_existing or not conv_parquet.exists():
                df_conv = _parse_convenios(files["convenios"], year)
                if df_conv is not None and not df_conv.is_empty():
                    df_conv.write_parquet(conv_parquet)
                    print(f"  {year}: {len(df_conv):,} convenios → {conv_parquet.name}")

    print("\nDone.")


# ── CLI ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Extract TransfereGov emendas-parlamentares (yearly ZIPs, 3-file structure)"
    )
    parser.add_argument(
        "--start-year", type=int, default=TRANSFEREGOV_START_YEAR,
        help=f"First year to extract (default: {TRANSFEREGOV_START_YEAR})",
    )
    parser.add_argument(
        "--end-year", type=int, default=None,
        help="Last year to extract (default: current year)",
    )
    parser.add_argument(
        "--skip-existing", action="store_true", default=True,
        help="Skip years whose Parquets already exist (default: True)",
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
