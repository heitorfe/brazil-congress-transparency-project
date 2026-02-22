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

import sys
from datetime import date
from pathlib import Path

import polars as pl

from api_client import SenateApiClient

if sys.stdout.encoding != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

RAW_DIR = Path("data/raw")
START_YEAR = 2019


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_br_decimal(value) -> float | None:
    """
    Convert a Brazilian locale numeric string to float.

    The ADM API returns monetary values formatted with Brazilian locale:
      '36.380,05'  → 36380.05   (thousands sep = '.', decimal sep = ',')
      '0,00'       → 0.0
      None         → None

    Standard float values (no comma) are also handled correctly.
    """
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    # Brazilian format: '36.380,05' → remove '.' → replace ',' with '.'
    if "," in s:
        s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Flattening helpers
# ---------------------------------------------------------------------------


def _flatten_servidor(rec: dict) -> dict:
    return {
        "sequencial":           rec.get("sequencial"),
        "nome":                 rec.get("nome"),
        "vinculo":              rec.get("vinculo"),
        "situacao":             rec.get("situacao"),
        "cargo_nome":           (rec.get("cargo") or {}).get("nome"),
        "padrao":               rec.get("padrao"),
        "especialidade":        rec.get("especialidade"),
        "funcao_nome":          (rec.get("funcao") or {}).get("nome"),
        "lotacao_sigla":        (rec.get("lotacao") or {}).get("sigla"),
        "lotacao_nome":         (rec.get("lotacao") or {}).get("nome"),
        "categoria_codigo":     (rec.get("categoria") or {}).get("codigo"),
        "categoria_nome":       (rec.get("categoria") or {}).get("nome"),
        "cedido_tipo":          (rec.get("cedido") or {}).get("tipo_cessao"),
        "cedido_orgao_origem":  (rec.get("cedido") or {}).get("orgao_origem"),
        "cedido_orgao_destino": (rec.get("cedido") or {}).get("orgao_destino"),
        "ano_admissao":         rec.get("ano_admissao"),
    }


def _flatten_pensionista(rec: dict) -> dict:
    return {
        "sequencial":         rec.get("sequencial"),
        "nome":               rec.get("nome"),
        "vinculo":            rec.get("vinculo"),
        "fundamento":         rec.get("fundamento"),
        "cargo_nome":         (rec.get("cargo") or {}).get("nome"),
        "funcao_nome":        (rec.get("funcao") or {}).get("nome"),
        "categoria_codigo":   (rec.get("categoria") or {}).get("codigo"),
        "categoria_nome":     (rec.get("categoria") or {}).get("nome"),
        "nome_instituidor":   rec.get("nome_instituidor"),
        "ano_exercicio":      rec.get("ano_exercicio"),
        "data_obito":         rec.get("data_obito"),
        "data_inicio_pensao": rec.get("data_inicio_pensao"),
    }


def _flatten_remuneracao(rec: dict, ano: int, mes: int) -> dict:
    return {
        "sequencial":                   rec.get("sequencial"),
        "nome":                         rec.get("nome"),
        "ano":                          ano,
        "mes":                          mes,
        "tipo_folha":                   rec.get("tipo_folha"),
        "remuneracao_basica":           rec.get("remuneracao_basica"),
        "vantagens_pessoais":           rec.get("vantagens_pessoais"),
        "funcao_comissionada":          rec.get("funcao_comissionada"),
        "gratificacao_natalina":        rec.get("gratificacao_natalina"),
        "horas_extras":                 rec.get("horas_extras"),
        "outras_eventuais":             rec.get("outras_eventuais"),
        "diarias":                      rec.get("diarias"),
        "auxilios":                     rec.get("auxilios"),
        "faltas":                       rec.get("faltas"),
        "previdencia":                  rec.get("previdencia"),
        "abono_permanencia":            rec.get("abono_permanencia"),
        "reversao_teto_constitucional": rec.get("reversao_teto_constitucional"),
        "imposto_renda":                rec.get("imposto_renda"),
        "remuneracao_liquida":          rec.get("remuneracao_liquida"),
        "vantagens_indenizatorias":     rec.get("vantagens_indenizatorias"),
    }


def _flatten_remuneracao_pensionista(rec: dict, ano: int, mes: int) -> dict:
    return {
        "sequencial":                   rec.get("sequencial"),
        "nome":                         rec.get("nome"),
        "ano":                          ano,
        "mes":                          mes,
        "tipo_folha":                   rec.get("tipo_folha"),
        "remuneracao_basica":           rec.get("remuneracao_basica"),
        "vantagens_pessoais":           rec.get("vantagens_pessoais"),
        "funcao_comissionada":          rec.get("funcao_comissionada"),
        "gratificacao_natalina":        rec.get("gratificacao_natalina"),
        "reversao_teto_constitucional": rec.get("reversao_teto_constitucional"),
        "imposto_renda":                rec.get("imposto_renda"),
        "remuneracao_liquida":          rec.get("remuneracao_liquida"),
        "vantagens_indenizatorias":     rec.get("vantagens_indenizatorias"),
        "previdencia":                  rec.get("previdencia"),
    }


def _flatten_hora_extra(rec: dict, ano: int, mes: int) -> dict:
    return {
        "sequencial":        rec.get("sequencial"),
        "nome":              rec.get("nome"),
        "valor_total":       rec.get("valorTotal"),
        "mes_ano_prestacao": rec.get("mes_ano_prestacao"),
        "mes_ano_pagamento": rec.get("mes_ano_pagamento"),
        "ano_pagamento":     ano,
        "mes_pagamento":     mes,
    }


# ---------------------------------------------------------------------------
# Month window generator — identical to extract_votacoes.py
# ---------------------------------------------------------------------------


def _month_windows(start: date, end: date) -> list[tuple[int, int]]:
    """Return a list of (year, month) tuples from start to end inclusive."""
    windows = []
    y, m = start.year, start.month
    while (y, m) <= (end.year, end.month):
        windows.append((y, m))
        m += 1
        if m > 12:
            m = 1
            y += 1
    return windows


# ---------------------------------------------------------------------------
# Extraction functions
# ---------------------------------------------------------------------------


def extract_servidores(client: SenateApiClient) -> None:
    print("Fetching staff registry (servidores)...", flush=True)
    data = client.get_adm("/api/v1/servidores/servidores")
    if not data:
        print("  empty — skipping")
        return
    if isinstance(data, dict):
        data = [data]

    records = [_flatten_servidor(r) for r in data if r]
    df = pl.DataFrame(records, infer_schema_length=len(records)).unique(subset=["sequencial"])

    out = RAW_DIR / "servidores.parquet"
    df.write_parquet(out)
    print(f"  Saved {len(df)} servidores → {out}")

    client.save_sample("servidores", data)


def extract_pensionistas(client: SenateApiClient) -> None:
    print("Fetching pensioner registry (pensionistas)...", flush=True)
    data = client.get_adm("/api/v1/servidores/pensionistas")
    if not data:
        print("  empty — skipping")
        return
    if isinstance(data, dict):
        data = [data]

    records = [_flatten_pensionista(r) for r in data if r]
    df = pl.DataFrame(records, infer_schema_length=len(records)).unique(subset=["sequencial"])
    out = RAW_DIR / "pensionistas.parquet"
    df.write_parquet(out)
    print(f"  Saved {len(df)} pensionistas → {out}")

    client.save_sample("pensionistas", data)


def extract_remuneracoes(
    client: SenateApiClient, start_year: int, end_year: int
) -> None:
    print(
        f"Fetching staff payroll (remuneracoes) {start_year}–{end_year}...", flush=True
    )
    windows = _month_windows(date(start_year, 1, 1), date(end_year, 12, 31))
    today = date.today()
    windows = [(y, m) for y, m in windows if date(y, m, 1) <= today]

    all_records: list[dict] = []
    for ano, mes in windows:
        print(f"  {ano}/{mes:02d}...", end=" ", flush=True)
        try:
            data = client.get_adm(f"/api/v1/servidores/remuneracoes/{ano}/{mes}")
            if not data:
                print("empty")
                continue
            if isinstance(data, dict):
                data = [data]
            records = [_flatten_remuneracao(r, ano, mes) for r in data if r]
            all_records.extend(records)
            print(f"{len(records)}")
        except Exception as e:
            print(f"ERROR: {e}")

    if not all_records:
        print("No remuneracoes data fetched.")
        return

    df = (
        pl.DataFrame(all_records, infer_schema_length=len(all_records))
        .unique(subset=["sequencial", "ano", "mes", "tipo_folha"])
        .sort(["ano", "mes", "sequencial"])
    )
    out = RAW_DIR / "remuneracoes_servidores.parquet"
    df.write_parquet(out)
    print(f"  Saved {len(df)} remuneracoes_servidores records → {out}")


def extract_remuneracoes_pensionistas(
    client: SenateApiClient, start_year: int, end_year: int
) -> None:
    print(
        f"Fetching pensioner payroll (pensionistas/remuneracoes) {start_year}–{end_year}...",
        flush=True,
    )
    windows = _month_windows(date(start_year, 1, 1), date(end_year, 12, 31))
    today = date.today()
    windows = [(y, m) for y, m in windows if date(y, m, 1) <= today]

    all_records: list[dict] = []
    for ano, mes in windows:
        print(f"  {ano}/{mes:02d}...", end=" ", flush=True)
        try:
            data = client.get_adm(
                f"/api/v1/servidores/pensionistas/remuneracoes/{ano}/{mes}"
            )
            if not data:
                print("empty")
                continue
            if isinstance(data, dict):
                data = [data]
            records = [_flatten_remuneracao_pensionista(r, ano, mes) for r in data if r]
            all_records.extend(records)
            print(f"{len(records)}")
        except Exception as e:
            print(f"ERROR: {e}")

    if not all_records:
        print("No pensionista remuneracoes data fetched.")
        return

    df = (
        pl.DataFrame(all_records, infer_schema_length=len(all_records))
        .unique(subset=["sequencial", "ano", "mes"])
        .sort(["ano", "mes", "sequencial"])
    )
    out = RAW_DIR / "remuneracoes_pensionistas.parquet"
    df.write_parquet(out)
    print(f"  Saved {len(df)} remuneracoes_pensionistas records → {out}")


def extract_horas_extras(
    client: SenateApiClient, start_year: int, end_year: int
) -> None:
    print(
        f"Fetching overtime (horas-extras) {start_year}–{end_year}...", flush=True
    )
    windows = _month_windows(date(start_year, 1, 1), date(end_year, 12, 31))
    today = date.today()
    windows = [(y, m) for y, m in windows if date(y, m, 1) <= today]

    all_records: list[dict] = []
    for ano, mes in windows:
        print(f"  {ano}/{mes:02d}...", end=" ", flush=True)
        try:
            data = client.get_adm(f"/api/v1/servidores/horas-extras/{ano}/{mes}")
            if not data:
                print("empty")
                continue
            if isinstance(data, dict):
                data = [data]
            records = [_flatten_hora_extra(r, ano, mes) for r in data if r]
            all_records.extend(records)
            print(f"{len(records)}")
        except Exception as e:
            print(f"ERROR: {e}")

    if not all_records:
        print("No horas-extras data fetched.")
        return

    df = (
        pl.DataFrame(all_records, infer_schema_length=len(all_records))
        .unique(subset=["sequencial", "ano_pagamento", "mes_pagamento"])
        .sort(["ano_pagamento", "mes_pagamento", "sequencial"])
    )
    out = RAW_DIR / "horas_extras.parquet"
    df.write_parquet(out)
    print(f"  Saved {len(df)} horas_extras records → {out}")

    client.save_sample("horas_extras", all_records)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def extract_all(start_year: int = START_YEAR, end_year: int | None = None) -> None:
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
        default=START_YEAR,
        help=f"First year to fetch monthly data (default: {START_YEAR})",
    )
    parser.add_argument(
        "--end-year",
        type=int,
        default=None,
        help="Last year to fetch monthly data (default: current year)",
    )
    args = parser.parse_args()
    extract_all(start_year=args.start_year, end_year=args.end_year)
