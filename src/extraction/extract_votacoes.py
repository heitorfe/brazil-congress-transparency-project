"""
Extract nominal voting records from the Brazilian Senate Open Data API.

Endpoint used:
  GET /votacao?dataInicio=YYYY-MM-DD&dataFim=YYYY-MM-DD
  -- Returns all plenary voting sessions in the date range, with all senator votes
     nested inside each session object.

Strategy:
  - Queries month-by-month from DEFAULT_START_DATE to today (or a supplied end date).
  - Each API response is a JSON array; votes are embedded in each session.
  - Session-level data → data/raw/votacoes.parquet
  - Senator-level vote data → data/raw/votos.parquet  (exploded from nested votos[])

See docs/raw_data_schemas.md for the full field-by-field schema documentation.
"""

from datetime import date

from api_client import SenateApiClient
from config import RAW_DIR, DEFAULT_START_DATE
from transforms.votacoes import flatten_votacao, flatten_voto
from utils import configure_utf8, save_parquet, month_date_windows

configure_utf8()


def fetch_window(
    client: SenateApiClient, window_start: date, window_end: date
) -> tuple[list[dict], list[dict]]:
    """Fetch all voting sessions for one date window; return (votacoes, votos)."""
    raw = client.get_legis(
        "/votacao",
        params={
            "dataInicio": window_start.isoformat(),
            "dataFim":    window_end.isoformat(),
        },
        suffix="",
    )

    # Response is usually a plain JSON array; handle dict wrapper defensively
    if isinstance(raw, list):
        sessions = raw
    elif isinstance(raw, dict) and "votacoes" in raw:
        sessions = raw["votacoes"]
    else:
        sessions = [raw] if raw else []

    votacoes = []
    votos = []
    for session in sessions:
        if not session or not isinstance(session, dict):
            continue
        codigo = session.get("codigoSessaoVotacao")
        if codigo is None:
            continue
        votacoes.append(flatten_votacao(session))
        for voto in session.get("votos") or []:
            votos.append(flatten_voto(codigo, voto))

    return votacoes, votos


def extract_all(start: date = DEFAULT_START_DATE, end: date | None = None) -> None:
    if end is None:
        end = date.today()

    RAW_DIR.mkdir(parents=True, exist_ok=True)

    windows = month_date_windows(start, end)
    print(f"Fetching {len(windows)} monthly windows from {start} to {end}...")

    all_votacoes: list[dict] = []
    all_votos: list[dict] = []

    with SenateApiClient() as client:
        for i, (w_start, w_end) in enumerate(windows, 1):
            label = f"[{i:>3}/{len(windows)}] {w_start} → {w_end}"
            try:
                votacoes, votos = fetch_window(client, w_start, w_end)
                all_votacoes.extend(votacoes)
                all_votos.extend(votos)
                print(f"  {label}  sessions={len(votacoes):>4}  votes={len(votos):>5}")
            except Exception as e:
                print(f"  {label}  ERROR: {e}")
        # SenateApiClient handles rate limiting — no manual time.sleep needed

    if not all_votacoes:
        print("No voting data fetched. Exiting.")
        return

    out_votacoes = RAW_DIR / "votacoes.parquet"
    out_votos    = RAW_DIR / "votos.parquet"

    n_v = save_parquet(all_votacoes, out_votacoes, unique_subset=["codigo_sessao_votacao"])
    n_vt = save_parquet(
        all_votos,
        out_votos,
        unique_subset=["codigo_sessao_votacao", "codigo_parlamentar"],
    )

    print(f"\nSaved {n_v} voting sessions → {out_votacoes}")
    print(f"Saved {n_vt} senator votes   → {out_votos}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Extract Senate nominal votes")
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
