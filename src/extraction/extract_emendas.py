"""Extract Parliamentary Amendments (Emendas Parlamentares) from Portal da Transparência (CGU).

Three datasets from https://portaldatransparencia.gov.br/download-de-dados/:
  1. emendas-parlamentares         — Aggregated by author × action × location (single ZIP)
  2. emendas-parlamentares-documentos — By SIAFI expense document (yearly ZIPs, 2014–current)
  3. apoiamento-emendas-parlamentares — Co-sponsor/supporter records (yearly ZIPs, 2020–2025)

ZIPs are streamed and extracted in-memory.
CSVs use semicolon (;) delimiter and Windows-1252 encoding.
All values are kept as strings in Parquet; type casting and BR decimal parsing happen in dbt staging.

Output (data/raw/):
  emendas_parlamentares.parquet   ← aggregated summary (all years, single file)
  emendas_documentos.parquet      ← by SIAFI document (2014–current year, yearly ZIPs)
  apoiamento_emendas.parquet      ← co-sponsors (2020–2025, yearly ZIPs)

Senator linking:
  codigo_emenda = YYYY + CCCC + NNNN (12 chars)
  codigo_autor_emenda = chars 5–8 = SIAFI author code
  Best-effort join against dim_senador.senador_id in dbt mart layer.
"""

import io
import time
import zipfile
from datetime import date
from pathlib import Path
import sys

import httpx
import polars as pl

sys.path.insert(0, str(Path(__file__).parent))
from config import RAW_DIR
from utils import configure_utf8

configure_utf8()

CGU_BASE = "https://dadosabertos-download.cgu.gov.br/PortalDaTransparencia/saida"

# ── Column rename maps (CSV Portuguese display names → snake_case) ──────────

EMENDAS_DOC_COLS = {
    "Código da Emenda": "codigo_emenda",
    "Ano da Emenda": "ano_emenda",
    "Código do Autor da Emenda": "codigo_autor_emenda",
    "Nome do Autor da Emenda": "nome_autor_emenda",
    "Número da emenda": "numero_emenda",
    "Valor Empenhado": "valor_empenhado",
    "Valor Pago": "valor_pago",
    "Tipo de Emenda": "tipo_emenda",
    "Data Documento": "data_documento",
    "Código Documento": "codigo_documento",
    "Localidade de aplicação do recurso": "localidade_recurso",
    "UF de aplicação do recurso": "uf_recurso",
    "Município de aplicação do recurso": "municipio_recurso",
    "Código IBGE do município de aplicação do recurso": "codigo_ibge_municipio",
    "Fase da despesa": "fase_despesa",
    "Código favorecido": "codigo_favorecido",
    "Favorecido": "favorecido",
    "Tipo Favorecido": "tipo_favorecido",
    "UF Favorecido": "uf_favorecido",
    "Município Favorecido": "municipio_favorecido",
    "Código UG": "codigo_ug",
    "UG": "ug",
    "Código Unidade Orçamentária": "codigo_unidade_orcamentaria",
    "Unidade Orçamentária": "unidade_orcamentaria",
    "Código Órgão SIAFI": "codigo_orgao",
    "Órgão": "orgao",
    "Código Órgão Superior SIAFI": "codigo_orgao_superior",
    "Órgão Superior": "orgao_superior",
    "Código Grupo Despesa": "codigo_grupo_despesa",
    "Grupo Despesa": "grupo_despesa",
    "Código Elemento Despesa": "codigo_elemento_despesa",
    "Elemento Despesa": "elemento_despesa",
    "Código Modalidade Aplicação Despesa": "codigo_modalidade_aplicacao",
    "Modalidade Aplicação Despesa": "modalidade_aplicacao",
    "Código Plano Orçamentário": "codigo_plano_orcamentario",
    "Plano Orçamentário": "plano_orcamentario",
    "Código Função": "codigo_funcao",
    "Função": "funcao",
    "Código SubFunção": "codigo_subfuncao",
    "SubFunção": "subfuncao",
    "Código Programa": "codigo_programa",
    "Programa": "programa",
    "Código Ação": "codigo_acao",
    "Ação": "acao",
    "Linguagem Cidadã": "linguagem_cidada",
    "Código Subtítulo (Localizador)": "codigo_subtitulo",
    "Subtítulo (Localizador)": "subtitulo",
    "Possui convênio?": "possui_convenio",
}

EMENDAS_COLS = {
    "Código da Emenda": "codigo_emenda",
    "Ano da Emenda": "ano_emenda",
    "Tipo de Emenda": "tipo_emenda",
    "Código do Autor da Emenda": "codigo_autor_emenda",
    "Nome do Autor da Emenda": "nome_autor_emenda",
    "Número da emenda": "numero_emenda",
    "Localidade de aplicação do recurso": "localidade_recurso",
    "Código Município IBGE": "codigo_municipio_ibge",
    "Município": "municipio",
    "Código UF IBGE": "codigo_uf_ibge",
    "UF": "uf",
    "Região": "regiao",
    "Código Função": "codigo_funcao",
    "Nome Função": "nome_funcao",
    "Código Subfunção": "codigo_subfuncao",
    "Nome Subfunção": "nome_subfuncao",
    "Código Programa": "codigo_programa",
    "Nome Programa": "nome_programa",
    "Código Ação": "codigo_acao",
    "Nome Ação": "nome_acao",
    "Código Plano Orçamentário": "codigo_plano_orcamentario",
    "Nome Plano Orçamentário": "nome_plano_orcamentario",
    "Valor Empenhado": "valor_empenhado",
    "Valor Liquidado": "valor_liquidado",
    "Valor Pago": "valor_pago",
    "Valor Restos A Pagar Inscritos": "valor_restos_inscrito",
    "Valor Restos A Pagar Cancelados": "valor_restos_cancelado",
    "Valor Restos A Pagar Pagos": "valor_restos_pagos",
}

APOIAMENTO_COLS = {
    "Código Apoiador": "codigo_apoiador",
    "Apoiador": "nome_apoiador",
    "Data do Apoio": "data_apoio",
    "Data Retirada do Apoio": "data_retirada_apoio",
    "Empenho": "empenho",
    "Data última movimentação Empenho": "data_ultima_movimentacao_empenho",
    "Código favorecido": "codigo_favorecido",
    "Favorecido": "favorecido",
    "Tipo Favorecido": "tipo_favorecido",
    "UF Favorecido": "uf_favorecido",
    "Município Favorecido": "municipio_favorecido",
    "Código da Emenda": "codigo_emenda",
    "Código do Autor da Emenda": "codigo_autor_emenda",
    "Nome do Autor da Emenda": "nome_autor_emenda",
    "Número da emenda": "numero_emenda",
    "Tipo de Emenda": "tipo_emenda",
    "Ano da Emenda": "ano_emenda",
    "Localidade de aplicação do recurso": "localidade_recurso",
    "Código UG": "codigo_ug",
    "UG": "ug",
    "Código Unidade Orçamentária": "codigo_unidade_orcamentaria",
    "Unidade Orçamentária": "unidade_orcamentaria",
    "Código Órgão SIAFI": "codigo_orgao",
    "Órgão": "orgao",
    "Código Órgão Superior SIAFI": "codigo_orgao_superior",
    "Órgão Superior": "orgao_superior",
    "Código Ação": "codigo_acao",
    "Ação": "acao",
    "Valor Empenhado": "valor_empenhado",
    "Valor Cancelado": "valor_cancelado",
    "Valor Pago": "valor_pago",
}

# ── HTTP helpers ─────────────────────────────────────────────────────────────


def _download(client: httpx.Client, urls: list[str]) -> bytes | None:
    """Try each URL in order, streaming the first successful response as bytes."""
    for url in urls:
        try:
            print(f"    GET {url}", flush=True)
            with client.stream("GET", url, follow_redirects=True, timeout=300) as r:
                if r.status_code == 404:
                    print("    → 404, trying next...")
                    continue
                r.raise_for_status()
                chunks = [chunk for chunk in r.iter_bytes(chunk_size=256 * 1024)]
                data = b"".join(chunks)
                print(f"    → {len(data) / 1024 / 1024:.1f} MB downloaded")
                return data
        except httpx.HTTPStatusError as e:
            print(f"    → HTTP {e.response.status_code}")
        except Exception as e:
            print(f"    → ERROR: {e}")
    return None


def _unzip_csv(data: bytes) -> bytes | None:
    """Extract the first CSV file from a ZIP archive, returning raw bytes."""
    try:
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            csvs = [n for n in zf.namelist() if n.lower().endswith(".csv")]
            if not csvs:
                print("    WARNING: no CSV found in ZIP")
                return None
            print(f"    Extracting {csvs[0]}...")
            return zf.read(csvs[0])
    except zipfile.BadZipFile as e:
        print(f"    ERROR: invalid ZIP — {e}")
        return None


def _read_csv(csv_bytes: bytes, col_map: dict[str, str]) -> pl.DataFrame:
    """Parse a semicolon-delimited Windows-1252 CSV and rename columns to snake_case.

    All columns are read as String (infer_schema_length=0). Type casting is done
    in dbt staging, following the same pattern used for ADM payroll data.
    """
    df = pl.read_csv(
        io.BytesIO(csv_bytes),
        separator=";",
        encoding="windows-1252",
        infer_schema_length=0,
        quote_char='"',
        truncate_ragged_lines=True,
        ignore_errors=True,
    )
    known = {k: v for k, v in col_map.items() if k in df.columns}
    if known:
        df = df.rename(known)
    return df


def _write_parquet(df: pl.DataFrame, path: Path, dedup_keys: list[str], sort_keys: list[str]) -> None:
    """Deduplicate, sort, and write a DataFrame to Parquet."""
    available_dedup = [k for k in dedup_keys if k in df.columns]
    if available_dedup:
        before = len(df)
        df = df.unique(subset=available_dedup)
        dupes = before - len(df)
        if dupes:
            print(f"    Deduplicated {dupes:,} duplicate rows")

    available_sort = [k for k in sort_keys if k in df.columns]
    if available_sort:
        df = df.sort(available_sort)

    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path)
    print(f"    Saved {len(df):,} rows → {path}")


# ── Dataset extractors ───────────────────────────────────────────────────────


def extract_emendas_resumo(client: httpx.Client) -> None:
    """Download the unified emendas summary (single ZIP covering all years)."""
    print("\nEmendas Parlamentares — resumo (single file)...", flush=True)
    urls = [
        f"{CGU_BASE}/emendas-parlamentares/EmendasParlamentares.zip",
    ]
    data = _download(client, urls)
    if not data:
        print("  SKIP: could not download emendas resumo")
        return

    csv_bytes = _unzip_csv(data)
    if not csv_bytes:
        return

    df = _read_csv(csv_bytes, EMENDAS_COLS)
    print(f"    {len(df):,} rows, {len(df.columns)} columns")
    print(f"    Columns: {df.columns}")

    _write_parquet(
        df,
        RAW_DIR / "emendas_parlamentares.parquet",
        dedup_keys=["codigo_emenda", "codigo_acao", "localidade_gasto"],
        sort_keys=["ano_emenda", "codigo_emenda"],
    )


def extract_emendas_documentos(client: httpx.Client, start_year: int, end_year: int) -> None:
    """Download yearly ZIPs of emendas por SIAFI document (2014–current year)."""
    print(f"\nEmendas por Documento ({start_year}–{end_year})...", flush=True)
    frames: list[pl.DataFrame] = []

    for year in range(start_year, end_year + 1):
        print(f"  {year}:", flush=True)
        urls = [
            f"{CGU_BASE}/emendas-parlamentares-documentos/{year}_EmendasParlamentaresPorDocumento.zip",
        ]
        data = _download(client, urls)
        if not data:
            print(f"    SKIP: no data for {year}")
            continue

        csv_bytes = _unzip_csv(data)
        if not csv_bytes:
            continue

        df = _read_csv(csv_bytes, EMENDAS_DOC_COLS)
        print(f"    {len(df):,} rows")
        frames.append(df)
        time.sleep(1)

    if not frames:
        print("  No emendas documentos data fetched.")
        return

    combined = pl.concat(frames, how="diagonal_relaxed")
    print(f"\n  Total before dedup: {len(combined):,} rows")
    _write_parquet(
        combined,
        RAW_DIR / "emendas_documentos.parquet",
        dedup_keys=["codigo_emenda", "codigo_documento", "fase_despesa"],
        sort_keys=["ano_emenda", "codigo_emenda"],
    )


def extract_apoiamento_emendas(client: httpx.Client, start_year: int, end_year: int) -> None:
    """Download yearly ZIPs of co-sponsor (apoiamento) records (2020–2025)."""
    print(f"\nApoiamento de Emendas ({start_year}–{end_year})...", flush=True)
    frames: list[pl.DataFrame] = []

    for year in range(start_year, end_year + 1):
        print(f"  {year}:", flush=True)
        urls = [
            f"{CGU_BASE}/emendas-parlamentares-apoiamento/{year}_ApoiamentoEmendasParlamentares.zip",
        ]
        data = _download(client, urls)
        if not data:
            print(f"    SKIP: no data for {year}")
            continue

        csv_bytes = _unzip_csv(data)
        if not csv_bytes:
            continue

        df = _read_csv(csv_bytes, APOIAMENTO_COLS)
        print(f"    {len(df):,} rows")
        frames.append(df)
        time.sleep(1)

    if not frames:
        print("  No apoiamento data fetched.")
        return

    combined = pl.concat(frames, how="diagonal_relaxed")
    print(f"\n  Total before dedup: {len(combined):,} rows")
    _write_parquet(
        combined,
        RAW_DIR / "apoiamento_emendas.parquet",
        dedup_keys=["empenho", "codigo_apoiador"],
        sort_keys=["ano_emenda", "empenho"],
    )


# ── Entry point ──────────────────────────────────────────────────────────────


def extract_all(
    start_doc_year: int = 2014,
    end_year: int | None = None,
    datasets: str = "all",
) -> None:
    if end_year is None:
        end_year = date.today().year

    RAW_DIR.mkdir(parents=True, exist_ok=True)

    with httpx.Client(
        timeout=httpx.Timeout(connect=30, read=300, write=30, pool=30),
        headers={"User-Agent": "Brazil-Congress-Dashboard/1.0 (civic-tech transparency)"},
    ) as client:
        if datasets in ("all", "resumo"):
            extract_emendas_resumo(client)
        if datasets in ("all", "documentos"):
            extract_emendas_documentos(client, start_doc_year, end_year)
        if datasets in ("all", "apoiamento"):
            apoiamento_start = max(start_doc_year, 2020)
            apoiamento_end = min(end_year, 2025)
            extract_apoiamento_emendas(client, apoiamento_start, apoiamento_end)

    print("\nAll emendas extractions complete.")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Extract Parliamentary Amendments from Portal da Transparência (CGU)"
    )
    parser.add_argument(
        "--start-year",
        type=int,
        default=2014,
        help="First year for documento and apoiamento yearly ZIPs (default: 2014)",
    )
    parser.add_argument(
        "--end-year",
        type=int,
        default=None,
        help="Last year to download (default: current year)",
    )
    parser.add_argument(
        "--datasets",
        choices=["all", "resumo", "documentos", "apoiamento"],
        default="all",
        help="Which datasets to download (default: all three)",
    )
    args = parser.parse_args()
    extract_all(start_doc_year=args.start_year, end_year=args.end_year, datasets=args.datasets)
