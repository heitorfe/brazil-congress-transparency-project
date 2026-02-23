"""
Extract senator biographical data from the Brazilian Senate Open Data API.

Endpoints used:
  GET /senador/lista/atual.json         -- list of senators currently in office
  GET /senador/{code}.json              -- biographical detail per senator
  GET /senador/{code}/mandatos.json     -- mandate period history per senator

Outputs:
  data/raw/senadores.parquet   -- one row per senator (biographical data)
  data/raw/mandatos.parquet    -- one row per mandate (a senator may have multiple)
"""

from api_client import SenateApiClient
from config import RAW_DIR
from transforms.senators import flatten_senator, flatten_mandate
from utils import configure_utf8, save_parquet, unwrap_list

configure_utf8()


def fetch_senator_list(client: SenateApiClient) -> list[dict]:
    data = client.get_legis("/senador/lista/atual")
    return data["ListaParlamentarEmExercicio"]["Parlamentares"]["Parlamentar"]


def fetch_senator_detail(client: SenateApiClient, code: str) -> dict:
    data = client.get_legis(f"/senador/{code}")
    parlamentar = data["DetalheParlamentar"]["Parlamentar"]
    return flatten_senator(parlamentar)


def fetch_mandatos(client: SenateApiClient, code: str) -> list[dict]:
    # Correct endpoint: /mandatos.json (plural, with .json suffix)
    data = client.get_legis(f"/senador/{code}/mandatos")
    raw_mandatos = (
        data.get("MandatoParlamentar", {})
            .get("Parlamentar", {})
            .get("Mandatos", {})
            .get("Mandato", [])
    )
    # API returns a single dict (not a list) when senator has only one mandate
    raw_mandatos = unwrap_list(raw_mandatos) if raw_mandatos else []
    return [flatten_mandate(code, m) for m in raw_mandatos]


def extract_all() -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    with SenateApiClient() as client:
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
        # SenateApiClient handles rate limiting — no manual time.sleep needed

    out_senators = RAW_DIR / "senadores.parquet"
    out_mandatos = RAW_DIR / "mandatos.parquet"

    n_sen = save_parquet(senators, out_senators)
    n_man = save_parquet(mandatos, out_mandatos)

    print(f"\nSaved {n_sen} senators  → {out_senators}")
    print(f"Saved {n_man} mandates → {out_mandatos}")


if __name__ == "__main__":
    extract_all()
