"""
Extract TSE (Tribunal Superior Eleitoral) electoral data for the Brazil Congress Dashboard.

Two datasets:
  candidates  — Candidate registrations (consulta_cand)
  donations   — Campaign donation receipts (prestacao_contas)

Source: https://cdn.tse.jus.br/estatistica/sead/odsele/
Format era: 2018+ (coded column names, consistent ZIP structure)
  - 2018, 2022: Federal elections  (Senators + Deputies elected)
  - 2024:       Municipal elections (Mayors + City Council — useful for donor cross-ref)

ZIP structure:
  Each ZIP contains one CSV per Brazilian state (UF), e.g.:
    consulta_cand_2022_SP.csv, consulta_cand_2022_RJ.csv, …, consulta_cand_2022_BRASIL.csv
  Strategy: concat all per-UF CSVs, skip the BRASIL aggregate to avoid duplicates.

Output:
  data/raw/tse_candidatos_{year}.parquet  — one per election year
  data/raw/tse_doacoes_{year}.parquet     — one per election year

Key quirks (from br-acc etl/scripts/download_tse.py analysis):
  - Encoding: latin-1, sep=';'
  - TSE 2024 masks all candidate CPFs as '-4' → set cpf_raw = None
  - VR_RECEITA is BRL locale string: "1.234,56" — kept raw, parsed in dbt staging
  - Stable donation_id: SHA256[:16] of sq_candidato|ano|cpf_cnpj|valor_raw|data_receita

CLI:
  python extract_tse.py --dataset candidates --years 2022
  python extract_tse.py --dataset donations  --years 2022 2024
  python extract_tse.py --dataset all        --years 2018 2022 2024 --skip-existing
  python extract_tse.py                      # default: all datasets, years 2018 2022 2024
"""

import hashlib
from datetime import date
from pathlib import Path

import polars as pl

from config import MASKED_CPF_SENTINEL, RAW_DIR, TSE_CDN_BASE, TSE_ELECTION_YEARS
from download_utils import download_file, safe_extract_zip, validate_csv
from utils import configure_utf8

configure_utf8()

# ── URL templates ─────────────────────────────────────────────────────────────

CAND_URL = TSE_CDN_BASE + "/consulta_cand/consulta_cand_{year}.zip"
DON_URL  = TSE_CDN_BASE + "/prestacao_contas/prestacao_de_contas_eleitorais_candidatos_{year}.zip"

# TSE donation ZIPs vary by election:
#   Federal (2018, 2022): ~3–4 GB uncompressed
#   Municipal (2024):     ~11 GB uncompressed (16K+ municipalities)
# Override the download_utils default of 2 GB accordingly.
_DON_MAX_BYTES = 15 * 1024**3   # 15 GB

# ── Column mappings (2018+ coded names) ───────────────────────────────────────

_CAND_COLS = {
    "SQ_CANDIDATO":       "sq_candidato",
    "NR_CPF_CANDIDATO":   "cpf_raw",
    "NM_CANDIDATO":       "nome_candidato",
    "NM_URNA_CANDIDATO":  "nome_urna",
    "DS_CARGO":           "cargo",
    "SG_UF":              "uf",
    "NM_UE":              "municipio",
    "ANO_ELEICAO":        "ano_eleicao",
    "SG_PARTIDO":         "partido_sigla",
    "NR_CANDIDATO":       "nr_candidato",
    "DS_SIT_TOT_TURNO":   "situacao_turno",
    "DS_GENERO":          "genero",
    "DS_GRAU_INSTRUCAO":  "grau_instrucao",
    "DS_OCUPACAO":        "ocupacao",
    "NR_IDADE_DATA_POSSE": "idade_posse",
}

_DON_COLS = {
    "SQ_CANDIDATO":           "sq_candidato",
    "NR_CPF_CNPJ_DOADOR":     "cpf_cnpj_doador_raw",
    "NM_DOADOR":              "nome_doador",
    "NM_DOADOR_RFB":          "nome_doador_rfb",
    "CD_CNAE_DOADOR":         "cnae_doador",
    "DS_CNAE_DOADOR":         "cnae_descricao",
    "NM_PARTIDO_DOADOR":      "partido_doador",
    "DS_ORIGEM_RECEITA":      "origem_receita",
    "DS_NATUREZA_RECEITA":    "natureza_receita",
    "DS_ESPECIE_RECEITA":     "especie_receita",
    "VR_RECEITA":             "valor_receita_raw",
    "DT_RECEITA":             "data_receita",
    "AA_ELEICAO":             "ano",
    "SG_UF":                  "uf",
}

_CAND_OUTPUT_COLS = [
    "sq_candidato", "ano_eleicao", "nome_candidato", "nome_urna", "cargo",
    "uf", "municipio", "partido_sigla", "nr_candidato", "situacao_turno",
    "genero", "grau_instrucao", "ocupacao", "idade_posse", "cpf_raw",
]

_DON_OUTPUT_COLS = [
    "donation_id", "sq_candidato", "ano", "uf",
    "cpf_cnpj_doador_raw", "nome_doador", "nome_doador_rfb",
    "cnae_doador", "cnae_descricao", "partido_doador",
    "origem_receita", "natureza_receita", "especie_receita",
    "valor_receita_raw", "data_receita",
]


# ── Helpers ───────────────────────────────────────────────────────────────────

def _stable_hash_col(composite: pl.Series, prefix: str) -> pl.Series:
    """Apply SHA256[:16] to every element of a string Series (same as Phase 4A)."""
    def _h(s: str) -> str:
        return hashlib.sha256(f"{prefix}_{s}".encode()).hexdigest()[:16]
    return composite.map_elements(_h, return_dtype=pl.Utf8)


def _mask_cpf(raw: str | None) -> str | None:
    """Return None if the CPF matches the TSE masked sentinel '-4'."""
    if not raw:
        return None
    # strip_document("-4") == "4" — 1 digit, clearly invalid; also guard the raw form
    stripped = "".join(c for c in raw if c.isdigit())
    if stripped == "4" or raw.strip() == MASKED_CPF_SENTINEL:
        return None
    return raw.strip()


def _download_zip(url: str, dest: Path, *, skip_existing: bool, label: str) -> Path | None:
    """Download a ZIP, skip if Parquet already exists (controlled by caller)."""
    if skip_existing and dest.exists():
        return dest
    print(f"  downloading {label}...", end=" ", flush=True)
    ok = download_file(url, dest)
    if not ok:
        print(f"failed, skipping")
        return None
    size_mb = dest.stat().st_size / (1024 * 1024)
    print(f"done ({size_mb:.1f} MB)")
    return dest


def _read_uf_csvs(
    extracted_paths: list[Path],
    filename_prefix: str,
    encoding: str = "latin1",
) -> pl.DataFrame | None:
    """Concat all per-UF CSVs from the extracted ZIP, skipping the BRASIL aggregate.

    TSE ZIPs contain one CSV per state (27 UFs) plus a BRASIL aggregate that would
    double-count every record if included. We identify UF files by prefix and exclude
    any file whose stem ends with '_BRASIL' (case-insensitive).
    """
    uf_files = [
        p for p in extracted_paths
        if p.stem.upper().startswith(filename_prefix.upper())
        and not p.stem.upper().endswith("_BRASIL")
        and p.suffix.lower() == ".csv"
    ]

    if not uf_files:
        print(f"  WARNING: no per-UF CSV files found matching '{filename_prefix}*'")
        return None

    dfs: list[pl.DataFrame] = []
    for csv_path in sorted(uf_files):
        # Quick validation before full parse
        if not validate_csv(csv_path, encoding=encoding, sep=";"):
            print(f"  WARNING: validation failed for {csv_path.name}, skipping")
            continue
        try:
            df = pl.read_csv(
                csv_path,
                encoding=encoding,
                separator=";",
                infer_schema=False,
                null_values=["", "N/A", "#NULO#"],
                truncate_ragged_lines=True,
            )
            if not df.is_empty():
                dfs.append(df)
        except Exception as e:
            print(f"  WARNING: parse error in {csv_path.name}: {e}")
            continue

    if not dfs:
        return None

    combined = pl.concat(dfs, how="diagonal")  # diagonal handles missing columns
    # Strip BOM from column names (occasional artifact in some UF files)
    combined = combined.rename({c: c.strip().lstrip("\ufeff").strip() for c in combined.columns})
    return combined


# ── Candidate extraction ──────────────────────────────────────────────────────

def _parse_candidates(df: pl.DataFrame, year: int) -> pl.DataFrame | None:
    """Normalize a raw candidate DataFrame into the canonical output schema."""
    # Rename to snake_case (only present columns)
    rename_map = {k: v for k, v in _CAND_COLS.items() if k in df.columns}
    df = df.rename(rename_map)

    # Inject ano_eleicao if not in CSV (should always be, but guard anyway)
    if "ano_eleicao" not in df.columns:
        df = df.with_columns(pl.lit(str(year)).alias("ano_eleicao"))

    # Fill nulls so operations don't propagate nulls unexpectedly
    df = df.with_columns([pl.col(c).fill_null("") for c in df.columns])

    # Apply CPF masking sentinel guard — sets cpf_raw to None when masked
    if "cpf_raw" in df.columns:
        df = df.with_columns(
            pl.col("cpf_raw")
            .map_elements(_mask_cpf, return_dtype=pl.Utf8)
            .alias("cpf_raw")
        )

    # Drop rows with empty sq_candidato (required FK in fct_doacao_eleitoral)
    if "sq_candidato" not in df.columns:
        print(f"  WARNING: 'sq_candidato' missing in candidates for {year}")
        return None
    df = df.filter(pl.col("sq_candidato").str.strip_chars().str.len_chars() > 0)

    if df.is_empty():
        return None

    # ── Validation stats ──
    total = len(df)
    masked_cpf = df.filter(pl.col("cpf_raw").is_null()).height if "cpf_raw" in df.columns else 0
    cargo_counts = (
        df.group_by("cargo").len().sort("len", descending=True).head(5)
        if "cargo" in df.columns else None
    )
    print(f"  {year}: {total:,} candidates | CPF masked: {masked_cpf:,} ({masked_cpf/total*100:.1f}%)")
    if cargo_counts is not None:
        top_cargos = ", ".join(
            f"{r['cargo']}={r['len']:,}" for r in cargo_counts.to_dicts()
        )
        print(f"         top cargos: {top_cargos}")

    # Ensure all output columns are present; add NULL placeholder for missing ones
    # (e.g. NR_IDADE_DATA_POSSE may be absent from some election years' CSVs)
    for col in _CAND_OUTPUT_COLS:
        if col not in df.columns:
            df = df.with_columns(pl.lit(None, dtype=pl.Utf8).alias(col))

    return df.select(_CAND_OUTPUT_COLS)


def extract_candidates(
    years: list[int] | None = None,
    *,
    skip_existing: bool = True,
) -> None:
    """Download and save candidate parquets for the given election years."""
    if years is None:
        years = TSE_ELECTION_YEARS

    zip_dir = RAW_DIR / "_tse_zips"
    csv_dir = RAW_DIR / "_tse_csvs"
    zip_dir.mkdir(parents=True, exist_ok=True)
    csv_dir.mkdir(parents=True, exist_ok=True)
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    print(f"\nExtracting TSE candidates: {years}")

    for year in years:
        out_parquet = RAW_DIR / f"tse_candidatos_{year}.parquet"

        if skip_existing and out_parquet.exists():
            print(f"  {year}: Parquet already exists, skipping")
            continue

        zip_dest = zip_dir / f"consulta_cand_{year}.zip"
        zip_path = _download_zip(
            CAND_URL.format(year=year),
            zip_dest,
            skip_existing=True,
            label=f"consulta_cand_{year}.zip",
        )
        if zip_path is None:
            continue

        print(f"  {year}: extracting ZIP...", end=" ", flush=True)
        csv_dir_year = csv_dir / f"cand_{year}"
        extracted = safe_extract_zip(zip_path, csv_dir_year)
        if not extracted:
            print("no files extracted")
            continue
        print(f"{len(extracted)} files")

        df = _read_uf_csvs(extracted, f"consulta_cand_{year}_")
        if df is None or df.is_empty():
            print(f"  {year}: no data after concat, skipping")
            continue

        df = _parse_candidates(df, year)
        if df is None or df.is_empty():
            print(f"  {year}: 0 records after parse, skipping")
            continue

        df = df.unique(subset=["sq_candidato"], keep="first")
        df = df.sort(["ano_eleicao", "uf", "cargo"])
        df.write_parquet(out_parquet)
        print(f"         → {out_parquet.name} saved ({len(df):,} unique candidates)")

    print("\nCandidates done.")


# ── Donation extraction ───────────────────────────────────────────────────────

def _parse_donations(df: pl.DataFrame, year: int) -> pl.DataFrame | None:
    """Normalize a raw donation DataFrame into the canonical output schema."""
    rename_map = {k: v for k, v in _DON_COLS.items() if k in df.columns}
    df = df.rename(rename_map)

    # Inject ano if not in CSV (AA_ELEICAO should always be present for 2018+)
    if "ano" not in df.columns:
        df = df.with_columns(pl.lit(str(year)).alias("ano"))

    # Fill nulls for concat_str
    df = df.with_columns([pl.col(c).fill_null("") for c in df.columns])

    # Drop rows without sq_candidato
    if "sq_candidato" not in df.columns:
        print(f"  WARNING: 'sq_candidato' missing in donations for {year}")
        return None
    df = df.filter(pl.col("sq_candidato").str.strip_chars().str.len_chars() > 0)

    if df.is_empty():
        return None

    # Build stable donation_id: SHA256[:16] of composite key
    sq   = pl.col("sq_candidato").str.strip_chars()
    ano  = pl.col("ano").str.strip_chars()
    cpf  = pl.col("cpf_cnpj_doador_raw").str.strip_chars() if "cpf_cnpj_doador_raw" in df.columns else pl.lit("")
    val  = pl.col("valor_receita_raw").str.strip_chars() if "valor_receita_raw" in df.columns else pl.lit("")
    dt   = pl.col("data_receita").str.strip_chars() if "data_receita" in df.columns else pl.lit("")

    df = df.with_columns(
        pl.concat_str([sq, ano, cpf, val, dt], separator="|").alias("_composite")
    )
    df = df.with_columns(
        _stable_hash_col(df["_composite"], prefix="tse_doacao").alias("donation_id")
    )
    df = df.drop("_composite")

    # ── Validation stats ──
    total = len(df)
    null_sq  = df.filter(pl.col("sq_candidato").str.strip_chars().str.len_chars() == 0).height
    null_cpf = df.filter(pl.col("cpf_cnpj_doador_raw").str.strip_chars().str.len_chars() == 0).height \
               if "cpf_cnpj_doador_raw" in df.columns else 0

    # Parse valor for stats only (don't keep — raw string preserved in Parquet)
    val_stats = ""
    if "valor_receita_raw" in df.columns:
        try:
            vals = df.select(
                pl.col("valor_receita_raw")
                .str.replace_all(r"\.", "")
                .str.replace(",", ".")
                .cast(pl.Float64, strict=False)
            ).to_series().drop_nulls()
            if len(vals) > 0:
                val_stats = (
                    f" | valor min={vals.min():,.2f} mean={vals.mean():,.2f} max={vals.max():,.2f}"
                )
        except Exception:
            pass

    print(
        f"  {year}: {total:,} donations | "
        f"sq_null={null_sq} cpf_null={null_cpf}{val_stats}"
    )

    return df.select([c for c in _DON_OUTPUT_COLS if c in df.columns])


def extract_donations(
    years: list[int] | None = None,
    *,
    skip_existing: bool = True,
) -> None:
    """Download and save donation parquets for the given election years."""
    if years is None:
        years = TSE_ELECTION_YEARS

    zip_dir = RAW_DIR / "_tse_zips"
    csv_dir = RAW_DIR / "_tse_csvs"
    zip_dir.mkdir(parents=True, exist_ok=True)
    csv_dir.mkdir(parents=True, exist_ok=True)
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    print(f"\nExtracting TSE donations: {years}")

    for year in years:
        out_parquet = RAW_DIR / f"tse_doacoes_{year}.parquet"

        if skip_existing and out_parquet.exists():
            print(f"  {year}: Parquet already exists, skipping")
            continue

        zip_dest = zip_dir / f"prestacao_contas_candidatos_{year}.zip"
        zip_path = _download_zip(
            DON_URL.format(year=year),
            zip_dest,
            skip_existing=True,
            label=f"prestacao_contas_candidatos_{year}.zip",
        )
        if zip_path is None:
            continue

        print(f"  {year}: extracting ZIP...", end=" ", flush=True)
        csv_dir_year = csv_dir / f"doacoes_{year}"
        extracted = safe_extract_zip(zip_path, csv_dir_year, max_total_bytes=_DON_MAX_BYTES)
        if not extracted:
            print("no files extracted")
            continue
        print(f"{len(extracted)} files")

        df = _read_uf_csvs(extracted, f"receitas_candidatos_{year}_")
        if df is None or df.is_empty():
            print(f"  {year}: no data after concat, skipping")
            continue

        df = _parse_donations(df, year)
        if df is None or df.is_empty():
            print(f"  {year}: 0 records after parse, skipping")
            continue

        df = df.unique(subset=["donation_id"], keep="first")
        df = df.sort(["ano", "sq_candidato"])
        df.write_parquet(out_parquet)
        print(f"         → {out_parquet.name} saved ({len(df):,} unique donations)")

    print("\nDonations done.")


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Extract TSE electoral data (candidates + donations, 2018+ era)"
    )
    parser.add_argument(
        "--dataset",
        choices=["candidates", "donations", "all"],
        default="all",
        help="Which dataset to extract (default: all)",
    )
    parser.add_argument(
        "--years",
        type=int,
        nargs="+",
        default=None,
        help=f"Election years to extract (default: {TSE_ELECTION_YEARS})",
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

    years = args.years or TSE_ELECTION_YEARS

    if args.dataset in ("candidates", "all"):
        extract_candidates(years, skip_existing=args.skip_existing)
    if args.dataset in ("donations", "all"):
        extract_donations(years, skip_existing=args.skip_existing)
