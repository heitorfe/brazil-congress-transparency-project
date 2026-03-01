"""
Senate extraction pipeline registry.

Run one or all extractors from a single entry point.

Usage:
    python src/extraction/pipeline.py                   # run all extractors
    python src/extraction/pipeline.py --only liderancas # run one
    python src/extraction/pipeline.py --only ceaps,processos --start-year 2022
    python src/extraction/pipeline.py --list            # show available extractors

Adding a new extractor:
    1. Create extract_myfeed.py with an extract_all() function following the
       existing convention (see extract_liderancas.py for the simplest example).
    2. Add one entry to REGISTRY below.
    3. Done — it appears automatically in --list and runs with --only myfeed.
"""

import argparse
import sys
from datetime import date
from typing import Callable

from utils import configure_utf8

configure_utf8()

# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------
# Each entry describes one extraction target:
#
#   "fn"   : the extract_all callable
#   "desc" : human-readable description (shown in --list)
#   "args" : list of argument tags this extractor accepts.
#            Supported tags: "start_year", "end_year", "start_date", "end_date"
#            Tags control which CLI flags are forwarded to the extractor.
# ---------------------------------------------------------------------------


def _build_registry() -> dict[str, dict]:
    # Lazy imports so individual scripts can still be run standalone without
    # importing every module in the package at once.
    import extract_senators
    import extract_votacoes
    import extract_comissoes
    import extract_liderancas
    import extract_processos
    import extract_ceaps
    import extract_servidores
    import extract_auxilio_moradia
    import extract_camara_deputados
    import extract_camara_despesas
    import extract_camara_proposicoes
    import extract_camara_votacoes

    return {
        "senators": {
            "fn":   extract_senators.extract_all,
            "desc": "Senator biographical profiles and mandate history (LEGIS)",
            "args": [],
        },
        "votacoes": {
            "fn":   extract_votacoes.extract_all,
            "desc": "Plenary voting sessions and senator votes (LEGIS)",
            "args": ["start_date", "end_date"],
        },
        "comissoes": {
            "fn":   extract_comissoes.extract_all,
            "desc": "Committee master list and senator memberships (LEGIS)",
            "args": [],
        },
        "liderancas": {
            "fn":   extract_liderancas.extract_all,
            "desc": "Current leadership positions (LEGIS)",
            "args": [],
        },
        "processos": {
            "fn":   extract_processos.extract_all,
            "desc": "Legislative proposals PL/PEC/PLP/MPV (LEGIS)",
            "args": ["start_year", "end_year"],
        },
        "ceaps": {
            "fn":   extract_ceaps.extract_all,
            "desc": "Senator CEAPS expense reimbursements (ADM)",
            "args": ["start_year", "end_year"],
        },
        "servidores": {
            "fn":   extract_servidores.extract_all,
            "desc": "Staff, pensioners, payroll, overtime (ADM)",
            "args": ["start_year", "end_year"],
        },
        "auxilio_moradia": {
            "fn":   extract_auxilio_moradia.extract_all,
            "desc": "Senator housing allowance snapshot (ADM)",
            "args": [],
        },
        # ---- Chamber of Deputies (Câmara dos Deputados) ----
        "camara_deputados": {
            "fn":   extract_camara_deputados.extract_all,
            "desc": "Deputy biographical profiles, legislatures 56+57 (CAMARA)",
            "args": [],
        },
        "camara_despesas": {
            "fn":   extract_camara_despesas.extract_all,
            "desc": "Deputy CEAP expense records (CAMARA)",
            "args": ["start_year", "end_year"],
        },
        "camara_proposicoes": {
            "fn":   extract_camara_proposicoes.extract_all,
            "desc": "Legislative proposals authored by deputies (CAMARA)",
            "args": ["start_year", "end_year"],
        },
        "camara_votacoes": {
            "fn":   extract_camara_votacoes.extract_all,
            "desc": "Plenary voting sessions and deputy votes (CAMARA)",
            "args": ["start_date", "end_date"],
        },
    }


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------


def _run_one(
    name: str,
    entry: dict,
    *,
    start_year: int,
    end_year: int,
    start_date: date,
    end_date: date,
) -> None:
    fn: Callable = entry["fn"]
    arg_tags: list[str] = entry["args"]

    kwargs: dict = {}
    if "start_year" in arg_tags:
        kwargs["start_year"] = start_year
    if "end_year" in arg_tags:
        kwargs["end_year"] = end_year
    if "start_date" in arg_tags:
        kwargs["start"] = start_date
    if "end_date" in arg_tags:
        kwargs["end"] = end_date

    print(f"\n{'=' * 60}")
    print(f"EXTRACTOR: {name}")
    print(f"{'=' * 60}")
    try:
        fn(**kwargs)
    except Exception as exc:
        print(f"FAILED [{name}]: {exc}")


def main() -> None:
    registry = _build_registry()

    parser = argparse.ArgumentParser(
        description="Run one or more Senate data extractors.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"Available extractors: {', '.join(registry)}",
    )
    parser.add_argument(
        "--only",
        metavar="NAME[,NAME...]",
        default=None,
        help="Comma-separated extractor names to run (default: all).",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List available extractors and exit.",
    )
    parser.add_argument(
        "--start-year",
        type=int,
        default=2019,
        metavar="YEAR",
        help="First year for year-range extractors (default: 2019).",
    )
    parser.add_argument(
        "--end-year",
        type=int,
        default=None,
        metavar="YEAR",
        help="Last year for year-range extractors (default: current year).",
    )
    parser.add_argument(
        "--start-date",
        default=None,
        metavar="YYYY-MM-DD",
        help="Start date for date-range extractors like votacoes (default: 2019-02-01).",
    )
    parser.add_argument(
        "--end-date",
        default=None,
        metavar="YYYY-MM-DD",
        help="End date for date-range extractors (default: today).",
    )
    args = parser.parse_args()

    if args.list:
        print("Available extractors:\n")
        for name, entry in registry.items():
            arg_info = f"  [{', '.join(entry['args'])}]" if entry["args"] else ""
            print(f"  {name:<20} {entry['desc']}{arg_info}")
        sys.exit(0)

    end_year = args.end_year or date.today().year
    start_date = date.fromisoformat(args.start_date) if args.start_date else date(2019, 2, 1)
    end_date = date.fromisoformat(args.end_date) if args.end_date else date.today()

    if args.only:
        names = [n.strip() for n in args.only.split(",")]
        unknown = [n for n in names if n not in registry]
        if unknown:
            print(f"ERROR: Unknown extractor(s): {unknown}")
            print(f"Available: {', '.join(registry)}")
            sys.exit(1)
        to_run = {n: registry[n] for n in names}
    else:
        to_run = registry

    for name, entry in to_run.items():
        _run_one(
            name,
            entry,
            start_year=args.start_year,
            end_year=end_year,
            start_date=start_date,
            end_date=end_date,
        )

    print("\nAll extractions complete.")


if __name__ == "__main__":
    main()
