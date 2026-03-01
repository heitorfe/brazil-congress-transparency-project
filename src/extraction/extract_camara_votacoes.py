"""
Extract plenary voting sessions and individual deputy votes from the Chamber API.

Endpoints used:
  GET /votacoes?dataInicio=YYYY-MM-DD&dataFim=YYYY-MM-DD&itens=100
    — All voting sessions in a date window.
  GET /votacoes/{id}/votos
    — Individual deputy votes for one session.

Strategy:
  - Queries month-by-month from start_date to today (Pattern D).
  - For each session returned, fetches all individual votes.
  - Session-level data → data/raw/camara_votacoes.parquet
  - Deputy vote data   → data/raw/camara_votos.parquet

Fetch pattern: Pattern D (ISO date query params, month windows) for sessions,
then one per-session call for votes.
"""

import argparse
from datetime import date

from camara_client import CamaraApiClient
from config import RAW_DIR
from transforms.camara_votacoes import flatten_votacao_camara, flatten_voto_camara
from utils import configure_utf8, save_parquet, month_date_windows

configure_utf8()

DEFAULT_START_DATE = date(2019, 2, 1)


def _fetch_votos(client: CamaraApiClient, votacao_id: str) -> list[dict]:
    """Fetch all individual votes for one voting session."""
    try:
        data = client.get(f"/votacoes/{votacao_id}/votos")
        records = data.get("dados") or []
        return [flatten_voto_camara(votacao_id, r) for r in records if r]
    except Exception as e:
        print(f"    votos ERROR [{votacao_id}]: {e}")
        return []


def extract_all(start: date = DEFAULT_START_DATE, end: date | None = None) -> None:
    if end is None:
        end = date.today()

    RAW_DIR.mkdir(parents=True, exist_ok=True)

    windows = month_date_windows(start, end)
    print(f"Fetching {len(windows)} monthly windows from {start} to {end}...")

    all_votacoes: list[dict] = []
    all_votos: list[dict] = []
    seen_votacoes: set[str] = set()  # prevent double-fetching if session spans months

    with CamaraApiClient() as client:
        for i, (w_start, w_end) in enumerate(windows, 1):
            label = f"[{i:>3}/{len(windows)}] {w_start} → {w_end}"
            try:
                records = client.get_all(
                    "/votacoes",
                    params={
                        "dataInicio": w_start.isoformat(),
                        "dataFim":    w_end.isoformat(),
                    },
                )
                new_sessions = [
                    flatten_votacao_camara(r) for r in records
                    if r and r.get("id") and r["id"] not in seen_votacoes
                ]

                for session in new_sessions:
                    vid = session["votacao_id"]
                    if vid:
                        seen_votacoes.add(vid)
                        all_votacoes.append(session)
                        all_votos.extend(_fetch_votos(client, str(vid)))

                print(
                    f"  {label}  new_sessions={len(new_sessions):>4}"
                    f"  total_votes={len(all_votos):>7}"
                )
            except Exception as e:
                print(f"  {label}  ERROR: {e}")

    if not all_votacoes:
        print("No voting data fetched.")
        return

    out_votacoes = RAW_DIR / "camara_votacoes.parquet"
    out_votos    = RAW_DIR / "camara_votos.parquet"

    n_v = save_parquet(
        all_votacoes,
        out_votacoes,
        unique_subset=["votacao_id"],
    )
    n_vt = save_parquet(
        all_votos,
        out_votos,
        unique_subset=["votacao_id", "deputado_id"],
        safe_schema=True,
    )

    print(f"\nSaved {n_v} voting sessions → {out_votacoes}")
    print(f"Saved {n_vt} deputy votes    → {out_votos}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Extract Chamber plenary voting sessions and deputy votes"
    )
    parser.add_argument(
        "--start",
        default=DEFAULT_START_DATE.isoformat(),
        help=f"Start date YYYY-MM-DD (default: {DEFAULT_START_DATE})",
    )
    parser.add_argument(
        "--end",
        default=None,
        help="End date YYYY-MM-DD (default: today)",
    )
    args = parser.parse_args()
    extract_all(
        start=date.fromisoformat(args.start),
        end=date.fromisoformat(args.end) if args.end else None,
    )
