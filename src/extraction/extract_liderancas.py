"""
Extract current leadership positions from the Brazilian Senate LEGIS API.

Endpoint used:
  GET /composicao/lideranca.json
  -- Returns a flat JSON array of ~314 leadership records.
  -- Each record identifies a senator/deputy in a government, party, or bloc
     leadership role (Líder or Vice-Líder).

Strategy:
  - Single API call — no pagination or date windowing needed.
  - Output: data/raw/liderancas.parquet

Key quirks:
  - ``codigoParlamentar`` is an integer — stored as string for FK join to dim_senador.
  - Not all records have a ``codigoPartido`` (leadership-specific party) — government
    leaders use ``codigoPartidoFiliacao`` (the senator's own party affiliation).
  - ``numeroOrdemViceLider`` is optional (None for primary leaders).
"""

from api_client import SenateApiClient
from config import RAW_DIR
from transforms.liderancas import flatten_lideranca_record
from utils import configure_utf8, save_parquet, unwrap_list

configure_utf8()


def extract_all() -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    print("Fetching leadership positions from /composicao/lideranca...", end=" ", flush=True)

    with SenateApiClient() as client:
        data = client.get_legis("/composicao/lideranca")

    data = unwrap_list(data)
    if not data:
        print("empty — no data returned.")
        return

    records = [flatten_lideranca_record(r) for r in data if r and r.get("codigo")]

    out = RAW_DIR / "liderancas.parquet"
    n = save_parquet(records, out, unique_subset=["codigo"])
    print(f"{n} leadership records → {out}")


if __name__ == "__main__":
    extract_all()
