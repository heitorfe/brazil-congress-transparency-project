"""
Extract deputy biographical data from the Brazilian Chamber of Deputies API.

Endpoints used:
  GET /deputados?idLegislatura={n}&itens=100   — list of deputies per legislature
  GET /deputados/{id}                          — biographical detail per deputy

Strategy:
  - Fetch the deputy list for each legislature in CAMARA_DEFAULT_LEGISLATURES
    (default: [56, 57], i.e. 2019-2023 and 2023-present).
  - Collect all unique deputy IDs across legislatures.
  - Fetch biographical detail for each unique ID.

Outputs:
  data/raw/camara_deputados_lista.parquet  — 1 row per deputy × legislature
  data/raw/camara_deputados.parquet        — 1 row per unique deputy (biography)

Fetch pattern: Pattern E (per-entity detail), adapted for a list-first approach.
"""

import argparse

from camara_client import CamaraApiClient
from config import RAW_DIR, CAMARA_DEFAULT_LEGISLATURES
from transforms.camara_deputados import flatten_deputado_list, flatten_deputado_detail
from utils import configure_utf8, save_parquet

configure_utf8()


def _fetch_lista(client: CamaraApiClient, legislatura_id: int) -> list[dict]:
    """Fetch and flatten all deputies for one legislature."""
    print(f"  Fetching legislature {legislatura_id}...", end=" ", flush=True)
    records = client.get_all("/deputados", params={"idLegislatura": legislatura_id})
    rows = [flatten_deputado_list(r, legislatura_id) for r in records if r]
    print(f"{len(rows)} deputies")
    return rows


def _fetch_detail(client: CamaraApiClient, deputado_id: str, label: str) -> dict | None:
    """Fetch and flatten biographical detail for one deputy."""
    try:
        data = client.get(f"/deputados/{deputado_id}")
        rec = data.get("dados") or {}
        return flatten_deputado_detail(rec) if rec else None
    except Exception as e:
        print(f"  {label}  ERROR: {e}")
        return None


def extract_all(legislaturas: list[int] | None = None) -> None:
    if legislaturas is None:
        legislaturas = CAMARA_DEFAULT_LEGISLATURES

    RAW_DIR.mkdir(parents=True, exist_ok=True)

    with CamaraApiClient() as client:
        # Step 1: Fetch the deputy list for each legislature
        all_lista: list[dict] = []
        for leg in legislaturas:
            all_lista.extend(_fetch_lista(client, leg))

        # Step 2: Get unique deputy IDs for detail fetch
        seen: set[str] = set()
        unique_ids: list[str] = []
        for row in all_lista:
            did = row["deputado_id"]
            if did and did not in seen:
                seen.add(did)
                unique_ids.append(did)

        print(f"\nFetching detail for {len(unique_ids)} unique deputies...")

        # Step 3: Fetch detail for each unique ID
        details: list[dict] = []
        for i, did in enumerate(unique_ids, 1):
            label = f"[{i:>4}/{len(unique_ids)}] deputy {did}"
            detail = _fetch_detail(client, did, label)
            if detail:
                details.append(detail)
                if i % 50 == 0 or i == len(unique_ids):
                    print(f"  ...{i}/{len(unique_ids)} fetched")

    # Save both outputs
    out_lista = RAW_DIR / "camara_deputados_lista.parquet"
    n_lista = save_parquet(
        all_lista,
        out_lista,
        unique_subset=["deputado_id", "id_legislatura"],
        sort_by=["id_legislatura", "deputado_id"],
        safe_schema=True,
    )
    print(f"\nSaved {n_lista} list rows    → {out_lista}")

    out_detail = RAW_DIR / "camara_deputados.parquet"
    n_detail = save_parquet(
        details,
        out_detail,
        unique_subset=["deputado_id"],
        sort_by=["deputado_id"],
        safe_schema=True,
    )
    print(f"Saved {n_detail} deputies    → {out_detail}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Extract Chamber deputy biographical profiles"
    )
    parser.add_argument(
        "--legislaturas",
        nargs="+",
        type=int,
        default=CAMARA_DEFAULT_LEGISLATURES,
        metavar="N",
        help=f"Legislature numbers to extract (default: {CAMARA_DEFAULT_LEGISLATURES})",
    )
    args = parser.parse_args()
    extract_all(legislaturas=args.legislaturas)
