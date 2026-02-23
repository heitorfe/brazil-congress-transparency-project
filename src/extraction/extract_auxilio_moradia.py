"""
Extract housing allowance (Auxílio-Moradia) data for Brazilian senators from the ADM API.

Endpoint used:
  GET /api/v1/senadores/auxilio-moradia
  -- Returns a flat snapshot of all senators with boolean flags for housing benefits.
  -- ~86 records total (one per senator).

Strategy:
  - Single API call — no pagination or date windowing needed.
  - Output: data/raw/auxilio_moradia.parquet

Key quirks:
  - No senator ID in the response — only ``nomeParlamentar`` (name) is available.
    Matching to dim_senador must be done by name in the dbt layer.
  - Boolean fields arrive as Python booleans from the JSON parser.
"""

from api_client import SenateApiClient
from config import RAW_DIR
from transforms.auxilio_moradia import flatten_auxilio_moradia_record
from utils import configure_utf8, save_parquet, unwrap_list

configure_utf8()


def extract_all() -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    print("Fetching housing allowance snapshot...", end=" ", flush=True)

    with SenateApiClient() as client:
        data = client.get_adm("/api/v1/senadores/auxilio-moradia")

    data = unwrap_list(data)
    if not data:
        print("empty — no data returned.")
        return

    records = [flatten_auxilio_moradia_record(r) for r in data if r]

    out = RAW_DIR / "auxilio_moradia.parquet"
    n = save_parquet(records, out, unique_subset=["nome_parlamentar"])
    print(f"{n} senators → {out}")


if __name__ == "__main__":
    extract_all()
