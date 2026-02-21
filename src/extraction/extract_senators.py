"""
Extract senator biographical data from the Brazilian Senate Open Data API.

Endpoints used:
  GET /senador/lista/atual.json         -> list of senators currently in office
  GET /senador/{code}.json              -> biographical detail per senator
  GET /senador/{code}/mandatos.json     -> mandate period history per senator

Outputs:
  data/raw/senadores.parquet   -- one row per senator (biographical data)
  data/raw/mandatos.parquet    -- one row per mandate (a senator may have multiple)
"""

import sys
import time
import httpx
import polars as pl
from pathlib import Path

# Force UTF-8 output on Windows to avoid cp1252 encoding errors
if sys.stdout.encoding != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

BASE_URL = "https://legis.senado.leg.br/dadosabertos"
RAW_DIR = Path("data/raw")


def _get(client: httpx.Client, url: str) -> dict:
    resp = client.get(url, timeout=30)
    resp.raise_for_status()
    return resp.json()


def fetch_senator_list(client: httpx.Client) -> list[dict]:
    data = _get(client, f"{BASE_URL}/senador/lista/atual.json")
    return data["ListaParlamentarEmExercicio"]["Parlamentares"]["Parlamentar"]


def _flatten_senator(raw: dict) -> dict:
    ident = raw.get("IdentificacaoParlamentar", {})
    dados = raw.get("DadosBasicosParlamentar", {})
    return {
        "senador_id":      str(ident.get("CodigoParlamentar", "")),
        "nome_parlamentar": ident.get("NomeParlamentar"),
        "nome_completo":   ident.get("NomeCompletoParlamentar"),
        "sexo":            ident.get("SexoParlamentar"),
        "foto_url":        ident.get("UrlFotoParlamentar"),
        "pagina_url":      ident.get("UrlPaginaParlamentar"),
        "email":           ident.get("EmailParlamentar"),
        "partido_sigla":   ident.get("SiglaPartidoParlamentar"),
        "estado_sigla":    ident.get("UfParlamentar"),
        "data_nascimento": dados.get("DataNascimento"),
        "naturalidade":    dados.get("Naturalidade"),
        "uf_naturalidade": dados.get("UfNaturalidade"),
    }


def fetch_senator_detail(client: httpx.Client, code: str) -> dict:
    data = _get(client, f"{BASE_URL}/senador/{code}.json")
    parlamentar = data["DetalheParlamentar"]["Parlamentar"]
    return _flatten_senator(parlamentar)


def _flatten_mandate(senador_id: str, mandato: dict) -> dict:
    # Each 8-year mandate spans two 4-year legislaturas.
    # mandato_inicio = PrimeiraLegislatura.DataInicio
    # mandato_fim    = SegundaLegislatura.DataFim
    leg1 = mandato.get("PrimeiraLegislaturaDoMandato", {})
    leg2 = mandato.get("SegundaLegislaturaDoMandato", {})
    return {
        "senador_id":             senador_id,
        "mandato_id":             str(mandato.get("CodigoMandato", "")),
        "estado_sigla":           mandato.get("UfParlamentar"),
        "data_inicio":            leg1.get("DataInicio"),
        "data_fim":               leg2.get("DataFim"),
        "legislatura_inicio":     str(leg1.get("NumeroLegislatura", "")),
        "legislatura_fim":        str(leg2.get("NumeroLegislatura", "")),
        "descricao_participacao": mandato.get("DescricaoParticipacao"),
    }


def fetch_mandatos(client: httpx.Client, code: str) -> list[dict]:
    # Correct endpoint: /mandatos.json (plural, with .json suffix)
    data = _get(client, f"{BASE_URL}/senador/{code}/mandatos.json")
    raw_mandatos = (
        data.get("MandatoParlamentar", {})
            .get("Parlamentar", {})
            .get("Mandatos", {})
            .get("Mandato", [])
    )
    # API returns a single dict (not a list) when senator has only one mandate
    if isinstance(raw_mandatos, dict):
        raw_mandatos = [raw_mandatos]
    return [_flatten_mandate(code, m) for m in raw_mandatos]


def extract_all():
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    with httpx.Client() as client:
        print("Fetching senator list...")
        senator_list = fetch_senator_list(client)
        codes = [
            s["IdentificacaoParlamentar"]["CodigoParlamentar"]
            for s in senator_list
        ]
        print(f"Found {len(codes)} senators in office.")

        senators = []
        mandatos = []

        for i, code in enumerate(codes, 1):
            print(f"  [{i}/{len(codes)}] Senator {code}...", end=" ", flush=True)
            try:
                senators.append(fetch_senator_detail(client, code))
                mandatos.extend(fetch_mandatos(client, code))
                print("OK")
            except Exception as e:
                print(f"ERROR: {e}")
            time.sleep(0.15)

    df_senators = pl.DataFrame(senators)
    df_mandatos = pl.DataFrame(mandatos)

    df_senators.write_parquet(RAW_DIR / "senadores.parquet")
    df_mandatos.write_parquet(RAW_DIR / "mandatos.parquet")

    print(f"\nSaved {len(df_senators)} senators  -> data/raw/senadores.parquet")
    print(f"Saved {len(df_mandatos)} mandates -> data/raw/mandatos.parquet")


if __name__ == "__main__":
    extract_all()
