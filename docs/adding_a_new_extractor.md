# Adding a New Data Extractor

**Last updated:** 2026-02-23
**Applies to:** `src/extraction/` architecture post-refactor

This guide walks through every step required to add a new data source to the
Brazil Senate Dashboard pipeline, from API exploration to Parquet output.

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Fetch Patterns](#2-fetch-patterns)
3. [Step-by-Step Checklist](#3-step-by-step-checklist)
4. [Step 1 — Explore the API Endpoint](#step-1--explore-the-api-endpoint)
5. [Step 2 — Write the Flatten Function](#step-2--write-the-flatten-function)
6. [Step 3 — Write the Extractor Script](#step-3--write-the-extractor-script)
7. [Step 4 — Register in pipeline.py](#step-4--register-in-pipelinepy)
8. [Step 5 — Verify Output](#step-5--verify-output)
9. [Complete Templates by Pattern](#complete-templates-by-pattern)
10. [Shared Utilities Reference](#shared-utilities-reference)
11. [Common Pitfalls](#common-pitfalls)

---

## 1. Architecture Overview

```
src/extraction/
├── api_client.py           — HTTP client (SenateApiClient)
├── config.py               — shared constants (RAW_DIR, DEFAULT_START_YEAR, …)
├── utils.py                — shared helpers (save_parquet, unwrap_list, …)
├── transforms/
│   ├── __init__.py         — re-exports all flatten functions
│   └── <domain>.py         — pure dict→dict flatten functions per domain
├── pipeline.py             — extractor registry + CLI runner
└── extract_<domain>.py     — fetch logic + extract_all() entrypoint
```

**Key rule: one responsibility per layer.**

| Layer | Responsibility | Contains I/O? |
|---|---|---|
| `api_client.py` | HTTP + rate limiting | Yes (network) |
| `transforms/<domain>.py` | Field mapping (dict→dict) | No |
| `utils.py` | Reusable helpers | Yes (filesystem) |
| `extract_<domain>.py` | Fetch orchestration | Yes (network + filesystem) |
| `pipeline.py` | Run coordination | Delegates to extractors |

---

## 2. Fetch Patterns

Every extractor follows one of five patterns. Identify yours before writing code.

| Pattern | When to use | Examples |
|---|---|---|
| **One-shot** | Single API call, no looping | `liderancas`, `auxilio_moradia` |
| **Year loop** | One call per year | `ceaps`, `processos` |
| **Month loop** | One call per calendar month | `votacoes`, `servidores` (payroll) |
| **Per-entity loop** | One call per senator/committee ID | `senators` (detail), `comissoes` (memberships) |
| **Multi-source merge** | Several endpoints merged in memory | `comissoes` (colegiados + mistas) |

Templates for each pattern are in [Section 9](#complete-templates-by-pattern).

---

## 3. Step-by-Step Checklist

```
[ ] 1. Explore endpoint — save sample JSON to data/api_sample/
[ ] 2. Write flatten function in transforms/<domain>.py
[ ] 3. Export from transforms/__init__.py
[ ] 4. Write extract_<domain>.py with extract_all()
[ ] 5. Add one entry to REGISTRY in pipeline.py
[ ] 6. Run python pipeline.py --only <name>
[ ] 7. Inspect output with polars
[ ] 8. (Optional) Add dbt staging model + tests
```

---

## Step 1 — Explore the API Endpoint

Use `SenateApiClient` interactively to understand the response shape before
writing any production code.

```python
# run from src/extraction/ directory
import sys; sys.path.insert(0, '.')
from api_client import SenateApiClient

with SenateApiClient() as client:
    # LEGIS API (legislative data)
    data = client.get_legis("/composicao/lideranca")

    # ADM API (administrative data)
    data = client.get_adm("/api/v1/senadores/auxilio-moradia")

    # Save a truncated sample for documentation
    client.save_sample("my_endpoint", data)  # → data/api_sample/my_endpoint.json
```

**Things to identify:**

- **Response envelope**: Is the array at the top level, or nested inside a dict?
  e.g. `data["ListaColegiados"]["Colegiados"]["Colegiado"]` vs. plain `data`
- **Singleton quirk**: If there is only one record, the LEGIS API may return a
  `dict` instead of a `list`. Always use `unwrap_list()` before iterating.
- **Empty responses**: Does the endpoint return `[]`, `{}`, `null`, or HTTP 200
  with an empty body when there is no data? Guard accordingly.
- **Field types**: Are integers that will serve as FK join keys? Cast them to
  `str`. Are monetary values Brazilian-locale strings like `"36.380,05"`? Leave
  them as raw strings (parsed in dbt staging) or use `parse_br_decimal()`.
- **Natural key for deduplication**: Which field(s) uniquely identify one row?

---

## Step 2 — Write the Flatten Function

Create (or add to) `src/extraction/transforms/<domain>.py`.
Flatten functions must be **pure** — no I/O, no network calls, no imports from
`api_client` or `utils`.

```python
# src/extraction/transforms/my_domain.py

def flatten_my_record(rec: dict) -> dict:
    """Flatten one record from GET /my/endpoint.

    Notes on any quirks (type coercions, missing fields, etc.).
    """
    return {
        "my_id":        str(rec.get("meuId") or ""),       # int → str (FK join)
        "nome":         rec.get("nome"),
        "valor":        rec.get("valor"),                  # keep as raw string
        "ativo":        bool(rec.get("ativo", False)),     # explicit bool
        "data_inicio":  rec.get("dataInicio"),             # ISO string; parsed in dbt
        "nested_field": (rec.get("nested") or {}).get("campo"),  # safe nested access
    }
```

**Conventions:**

- Use `str(rec.get("CodXxx") or "")` for integer ID fields (the `or ""` handles
  `None` before `str()` to avoid `"None"` strings).
- Use `(rec.get("nested") or {}).get("field")` for optional nested objects —
  avoids `AttributeError` when the parent key is absent.
- Leave monetary values as raw strings if they come from the ADM API in
  Brazilian locale format. Parse with `REPLACE()/CAST` in dbt staging.

**Export the function** from `transforms/__init__.py`:

```python
# add to transforms/__init__.py
from .my_domain import flatten_my_record

__all__ = [
    ...,
    "flatten_my_record",
]
```

---

## Step 3 — Write the Extractor Script

Create `src/extraction/extract_<domain>.py`. The script must:

1. Import flatten function(s) from `transforms/`
2. Import `SenateApiClient` from `api_client`
3. Import `RAW_DIR` (and optionally `DEFAULT_START_YEAR`) from `config`
4. Import helpers from `utils`
5. Call `configure_utf8()` at module level
6. Define `extract_all(**kwargs)` as the public entrypoint
7. Keep a `if __name__ == "__main__":` block with argparse for standalone use

See complete templates in [Section 9](#complete-templates-by-pattern).

---

## Step 4 — Register in pipeline.py

Open `src/extraction/pipeline.py` and add one entry to the dict returned by
`_build_registry()`:

```python
def _build_registry() -> dict[str, dict]:
    ...
    import extract_my_domain          # add import

    return {
        ...
        "my_domain": {                               # CLI name
            "fn":   extract_my_domain.extract_all,  # callable
            "desc": "My domain description (LEGIS)", # shown in --list
            "args": [],                              # [] | ["start_year","end_year"] | ["start_date","end_date"]
        },
    }
```

**`"args"` values:**

| Tag | What gets forwarded | Extractor signature |
|---|---|---|
| `[]` | nothing | `extract_all()` |
| `["start_year", "end_year"]` | `start_year=int, end_year=int` | `extract_all(start_year, end_year)` |
| `["start_date", "end_date"]` | `start=date, end=date` | `extract_all(start, end)` |

---

## Step 5 — Verify Output

```bash
# Run from repo root
source .venv/Scripts/activate

# Run just the new extractor
python src/extraction/pipeline.py --only my_domain

# Or run it standalone (useful during development)
python src/extraction/extract_my_domain.py

# Inspect the output
python -c "
import polars as pl
df = pl.read_parquet('data/raw/my_domain.parquet')
print(df.shape)
print(df.dtypes)
print(df.head(5))
"
```

---

## Complete Templates by Pattern

### Pattern A — One-shot (single API call)

Use for: snapshot endpoints that return all data in a single response.
Examples: `liderancas`, `auxilio_moradia`.

```python
"""
Extract <description> from the Brazilian Senate <LEGIS|ADM> API.

Endpoint used:
  GET /my/endpoint
  -- <describe the response>.

Strategy:
  - Single API call — no pagination or date windowing needed.
  - Output: data/raw/<name>.parquet
"""

from api_client import SenateApiClient
from config import RAW_DIR
from transforms.my_domain import flatten_my_record
from utils import configure_utf8, save_parquet, unwrap_list

configure_utf8()


def extract_all() -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    print("Fetching <name>...", end=" ", flush=True)

    with SenateApiClient() as client:
        data = client.get_legis("/my/endpoint")        # or .get_adm(...)

    data = unwrap_list(data)
    if not data:
        print("empty — no data returned.")
        return

    records = [flatten_my_record(r) for r in data if r]

    out = RAW_DIR / "<name>.parquet"
    n = save_parquet(records, out, unique_subset=["my_id"])
    print(f"{n} records → {out}")


if __name__ == "__main__":
    extract_all()
```

---

### Pattern B — Year loop

Use for: endpoints that take a year parameter and return all records for that year.
Examples: `ceaps`, `processos`.

```python
"""..."""

from datetime import date

from api_client import SenateApiClient
from config import RAW_DIR, DEFAULT_START_YEAR
from transforms.my_domain import flatten_my_record
from utils import configure_utf8, save_parquet, unwrap_list

configure_utf8()


def extract_all(start_year: int = DEFAULT_START_YEAR, end_year: int | None = None) -> None:
    if end_year is None:
        end_year = date.today().year

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    all_records: list[dict] = []

    with SenateApiClient() as client:
        for year in range(start_year, end_year + 1):
            print(f"  {year}...", end=" ", flush=True)
            try:
                data = client.get_adm(f"/api/v1/my/endpoint/{year}")
                data = unwrap_list(data)
                if not data:
                    print("empty")
                    continue
                records = [flatten_my_record(r) for r in data if r]
                all_records.extend(records)
                print(f"{len(records)}")
            except Exception as e:
                print(f"ERROR: {e}")

    if not all_records:
        print("No data fetched.")
        return

    out = RAW_DIR / "<name>.parquet"
    n = save_parquet(
        all_records,
        out,
        unique_subset=["my_id"],
        sort_by=["ano", "my_id"],
    )
    print(f"\nSaved {n} records → {out}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--start-year", type=int, default=DEFAULT_START_YEAR)
    parser.add_argument("--end-year", type=int, default=None)
    args = parser.parse_args()
    extract_all(start_year=args.start_year, end_year=args.end_year)
```

---

### Pattern C — Month loop (integer year/month URL segments)

Use for: ADM endpoints with `/{ano}/{mes}` path segments.
Examples: `servidores` (remuneracoes), `horas_extras`.

```python
"""..."""

from datetime import date

from api_client import SenateApiClient
from config import RAW_DIR, DEFAULT_START_YEAR
from transforms.my_domain import flatten_my_record
from utils import configure_utf8, save_parquet, unwrap_list, month_windows

configure_utf8()


def extract_all(start_year: int = DEFAULT_START_YEAR, end_year: int | None = None) -> None:
    if end_year is None:
        end_year = date.today().year

    RAW_DIR.mkdir(parents=True, exist_ok=True)

    # month_windows() caps at today automatically — never fetches future months
    windows = month_windows(date(start_year, 1, 1), date(end_year, 12, 31))
    all_records: list[dict] = []

    with SenateApiClient() as client:
        for ano, mes in windows:
            print(f"  {ano}/{mes:02d}...", end=" ", flush=True)
            try:
                data = client.get_adm(f"/api/v1/my/endpoint/{ano}/{mes}")
                data = unwrap_list(data)
                if not data:
                    print("empty")
                    continue
                records = [flatten_my_record(r) for r in data if r]
                all_records.extend(records)
                print(f"{len(records)}")
            except Exception as e:
                print(f"ERROR: {e}")

    if not all_records:
        print("No data fetched.")
        return

    out = RAW_DIR / "<name>.parquet"
    # Use safe_schema=True if any string fields are absent for the first N records
    # (prevents Polars Null-type inference failure — see utils.save_parquet docstring)
    n = save_parquet(
        all_records,
        out,
        unique_subset=["my_id", "ano", "mes"],
        sort_by=["ano", "mes", "my_id"],
        safe_schema=True,
    )
    print(f"\nSaved {n} records → {out}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--start-year", type=int, default=DEFAULT_START_YEAR)
    parser.add_argument("--end-year", type=int, default=None)
    args = parser.parse_args()
    extract_all(start_year=args.start_year, end_year=args.end_year)
```

---

### Pattern D — Month loop (ISO date query params)

Use for: LEGIS endpoints with `?dataInicio=YYYY-MM-DD&dataFim=YYYY-MM-DD`.
Examples: `votacoes`.

```python
"""..."""

from datetime import date

from api_client import SenateApiClient
from config import RAW_DIR, DEFAULT_START_DATE
from transforms.my_domain import flatten_my_record
from utils import configure_utf8, save_parquet, unwrap_list, month_date_windows

configure_utf8()


def extract_all(start: date = DEFAULT_START_DATE, end: date | None = None) -> None:
    if end is None:
        end = date.today()

    RAW_DIR.mkdir(parents=True, exist_ok=True)

    windows = month_date_windows(start, end)
    print(f"Fetching {len(windows)} monthly windows...")
    all_records: list[dict] = []

    with SenateApiClient() as client:
        for i, (w_start, w_end) in enumerate(windows, 1):
            label = f"[{i:>3}/{len(windows)}] {w_start} → {w_end}"
            try:
                # suffix="" because /my-endpoint uses query params, not .json suffix
                data = client.get_legis(
                    "/my-endpoint",
                    params={"dataInicio": w_start.isoformat(), "dataFim": w_end.isoformat()},
                    suffix="",
                )
                data = unwrap_list(data)
                if not data:
                    print(f"  {label}  (empty)")
                    continue
                records = [flatten_my_record(r) for r in data if r]
                all_records.extend(records)
                print(f"  {label}  {len(records)} records")
            except Exception as e:
                print(f"  {label}  ERROR: {e}")

    if not all_records:
        print("No data fetched.")
        return

    out = RAW_DIR / "<name>.parquet"
    n = save_parquet(all_records, out, unique_subset=["my_id"])
    print(f"\nSaved {n} records → {out}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--start", default=DEFAULT_START_DATE.isoformat())
    parser.add_argument("--end", default=None)
    args = parser.parse_args()
    extract_all(
        start=date.fromisoformat(args.start),
        end=date.fromisoformat(args.end) if args.end else None,
    )
```

---

### Pattern E — Per-entity loop

Use for: endpoints that require one call per senator/entity ID.
Examples: `senators` (detail + mandatos), `comissoes` (membership history).

```python
"""..."""

import polars as pl

from api_client import SenateApiClient
from config import RAW_DIR
from transforms.my_domain import flatten_my_record
from utils import configure_utf8, save_parquet, unwrap_list

configure_utf8()


def _get_senator_ids(client: SenateApiClient) -> list[str]:
    """Load IDs from existing senadores.parquet, or fetch from API."""
    parquet = RAW_DIR / "senadores.parquet"
    if parquet.exists():
        return pl.read_parquet(parquet)["senador_id"].drop_nulls().unique().to_list()
    data = client.get_legis("/senador/lista/atual")
    senators = unwrap_list(
        data.get("ListaParlamentarEmExercicio", {})
            .get("Parlamentares", {})
            .get("Parlamentar")
    )
    return [
        str(s.get("IdentificacaoParlamentar", {}).get("CodigoParlamentar", ""))
        for s in senators
        if s.get("IdentificacaoParlamentar", {}).get("CodigoParlamentar")
    ]


def extract_all() -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    with SenateApiClient() as client:
        senator_ids = _get_senator_ids(client)
        print(f"Fetching data for {len(senator_ids)} senators...")

        all_records: list[dict] = []
        for i, senador_id in enumerate(senator_ids, 1):
            label = f"[{i:>3}/{len(senator_ids)}] senator {senador_id}"
            try:
                data = client.get_legis(f"/senador/{senador_id}/my-endpoint")
                # Unwrap API envelope first, then iterate
                items = unwrap_list(
                    data.get("EnvelopeKey", {})
                        .get("NestedKey", {})
                        .get("ItemList")
                )
                records = [flatten_my_record(senador_id, item) for item in items]
                all_records.extend(records)
                print(f"  {label}  {len(records)} records")
            except Exception as e:
                print(f"  {label}  ERROR: {e}")

    if not all_records:
        print("No data fetched.")
        return

    out = RAW_DIR / "<name>.parquet"
    n = save_parquet(
        all_records,
        out,
        unique_subset=["senador_id", "my_id"],
    )
    print(f"\nSaved {n} records → {out}")


if __name__ == "__main__":
    extract_all()
```

---

## Shared Utilities Reference

All helpers live in `src/extraction/utils.py`.

### `configure_utf8()`

Call at module level in every extractor. Forces UTF-8 stdout on Windows to
avoid `cp1252` encoding errors with Portuguese text.

```python
configure_utf8()  # call once, at the top of the module body
```

### `unwrap_list(obj)`

Normalises the three possible API response shapes into a guaranteed `list`:

```python
unwrap_list(None)          # → []
unwrap_list({"a": 1})      # → [{"a": 1}]   (XML→JSON singleton quirk)
unwrap_list([{"a": 1}])    # → [{"a": 1}]
```

Always call before iterating over any LEGIS API response array.

### `save_parquet(records, path, *, unique_subset, sort_by, safe_schema)`

```python
n = save_parquet(
    records,                          # list[dict]
    RAW_DIR / "my_file.parquet",      # output path (parent created automatically)
    unique_subset=["id"],             # dedup columns (omit to skip dedup)
    sort_by=["ano", "mes", "id"],     # sort order (omit to skip sort)
    safe_schema=False,                # set True for ADM payroll/staff data
                                      # (see Polars null-type inference pitfall below)
)
print(f"{n} rows written")
```

Returns the number of rows written after deduplication.

### `month_windows(start, end)` → `list[tuple[int, int]]`

For ADM endpoints with `/{ano}/{mes}` URL segments:

```python
windows = month_windows(date(2024, 1, 1), date(2025, 12, 31))
# → [(2024,1), (2024,2), ..., (2025,2)]   ← capped at today (Feb 2026)

for ano, mes in windows:
    data = client.get_adm(f"/api/v1/something/{ano}/{mes}")
```

### `month_date_windows(start, end)` → `list[tuple[date, date]]`

For LEGIS endpoints with ISO date query params:

```python
windows = month_date_windows(date(2024, 1, 1), date(2025, 12, 31))
# → [(date(2024,1,1), date(2024,1,31)), ..., (date(2025,2,1), date(2025,2,23))]

for w_start, w_end in windows:
    data = client.get_legis("/endpoint", params={
        "dataInicio": w_start.isoformat(),
        "dataFim":    w_end.isoformat(),
    }, suffix="")
```

### `parse_br_decimal(value)` → `float | None`

Converts Brazilian locale monetary strings to float. Use only if you need
numeric values at extraction time; otherwise leave them as raw strings and
parse in dbt staging with `REPLACE()/CAST`.

```python
parse_br_decimal("36.380,05")  # → 36380.05
parse_br_decimal("0,00")       # → 0.0
parse_br_decimal(None)         # → None
```

---

## Common Pitfalls

### 1. Singleton dict instead of list (LEGIS API XML→JSON)

The LEGIS API converts XML to JSON. When a collection has only one element,
it returns a `dict` instead of a `[dict]`.

```python
# WRONG — crashes when there is only one record
for item in data.get("MyList", {}).get("MyItem"):
    ...

# RIGHT
items = unwrap_list(data.get("MyList", {}).get("MyItem"))
for item in items:
    ...
```

### 2. Polars Null-type inference with optional string fields

When an optional field is `None` for the first N records, Polars infers `Null`
type for that column. If a real string arrives later in the list, Polars raises
`SchemaError`.

```python
# WRONG — may fail for ADM staff/payroll data
df = pl.DataFrame(records)

# RIGHT — use safe_schema=True in save_parquet, or pass directly:
df = pl.DataFrame(records, infer_schema_length=len(records))
```

Use `save_parquet(..., safe_schema=True)` for any ADM endpoint where optional
string fields (cargo, funcao, lotacao, cedido) may be absent for the first rows.

### 3. Integer IDs that join VARCHAR columns

Several API fields are integers (`codigoParlamentar`, `codSenador`) but must
join `dim_senador.senador_id` which is VARCHAR. Always cast to string in the
flatten function:

```python
"senador_id": str(rec.get("codigoParlamentar") or ""),
#              ^^^                              ^^^^^
#              str() wraps the int             or "" avoids "None" string
```

### 4. Empty HTTP 200 responses

Some LEGIS endpoints return HTTP 200 with an empty body `b""` when no records
exist for a given date range. `SenateApiClient._get()` will raise
`json.JSONDecodeError` in this case. Wrap in `try/except` and continue:

```python
try:
    data = client.get_legis(...)
    data = unwrap_list(data)
    if not data:
        continue
    ...
except Exception as e:
    print(f"ERROR: {e}")
```

### 5. Brazilian locale decimal strings (ADM API)

The ADM API returns ALL monetary values as Brazilian-locale strings:
`"36.380,05"` (`.` = thousands sep, `,` = decimal sep). `CAST` in DuckDB will
fail on this format.

**Strategy:** Keep raw strings in Parquet. Parse in dbt staging:

```sql
-- in dbt staging model
TRY_CAST(
    REPLACE(REPLACE(valor_reembolsado::varchar, '.', ''), ',', '.')
    AS DECIMAL(12,2)
) AS valor_reembolsado
```

### 6. The `/votacao` and `/processo` endpoints don't use `.json` suffix

These LEGIS endpoints use query params and return JSON without the `.json` URL
suffix. Pass `suffix=""` explicitly:

```python
data = client.get_legis("/votacao", params={...}, suffix="")
data = client.get_legis("/processo", params={...}, suffix="")
```

### 7. `DEFAULT_START_DATE` vs `DEFAULT_START_YEAR`

- Use `DEFAULT_START_DATE = date(2019, 2, 1)` for date-windowed extractors
  (pattern D) — this is when the LEGIS votacao endpoint has reliable data.
- Use `DEFAULT_START_YEAR = 2019` for year-loop and month-loop extractors
  (patterns B and C).

Both are exported from `config.py`.

---

## Quick Reference: `SenateApiClient` Methods

```python
with SenateApiClient() as client:

    # LEGIS API — appends .json suffix by default
    data = client.get_legis("/senador/lista/atual")
    data = client.get_legis(f"/senador/{code}/comissoes")

    # LEGIS API — no suffix (endpoint uses query params)
    data = client.get_legis("/votacao", params={"dataInicio": "2024-01-01", ...}, suffix="")
    data = client.get_legis("/processo", params={"sigla": "PL", "ano": 2024}, suffix="")

    # ADM API — no suffix handling needed
    data = client.get_adm("/api/v1/servidores/servidores")
    data = client.get_adm(f"/api/v1/servidores/remuneracoes/{ano}/{mes}")

    # Save a 5-record sample to data/api_sample/<name>.json
    client.save_sample("my_endpoint", data)
```

Rate limiting is automatic: 0.15 s after each LEGIS request, 0.3 s after each ADM request.
