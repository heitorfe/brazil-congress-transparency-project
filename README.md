# Brazil Senate Dashboard

A civic-tech transparency portal for the Brazilian Federal Senate (*Senado Federal*).

Aggregates open government data from the [Senate Open Data API](https://legis.senado.leg.br/dadosabertos/)
into a structured analytical warehouse and exposes it through an interactive Streamlit dashboard.

---

## Architecture

```
Senate Open Data API
        │
        ▼  httpx + Polars
src/extraction/
  extract_senators.py     ← pulls JSON, flattens, writes Parquet
        │
        ▼  Parquet (bronze layer)
data/raw/
  senadores.parquet       ← 1 row per senator
  mandatos.parquet        ← 1 row per mandate
        │
        ▼  dbt-duckdb
dbt_project/
  staging/                ← stg_senadores, stg_mandatos (views)
  marts/                  ← dim_senador, dim_partido (tables)
  seeds/partidos.csv      ← 30 TSE-registered parties (reference data)
        │
        ▼  DuckDB
data/warehouse/senate.duckdb
  main_staging.*
  main_marts.*
  main_seeds.*
        │
        ▼  Streamlit
dashboard/
  app.py                  ← senator list + filters + metrics
  pages/1_Perfil_do_Senador.py  ← individual senator profile
```

---

## Tech Stack

| Layer | Tool |
|---|---|
| Extraction | Python + `httpx` + `polars` |
| Transformation | `dbt-duckdb` 1.10.1 / dbt-core 1.11.6 |
| Warehouse | DuckDB (local file) |
| Dashboard | Streamlit + Polars |
| Package manager | `uv` (isolated `.venv`) |

---

## Quickstart

> **Important (Windows):** The system `dbt` resolves to dbt-fusion 2.0 which does NOT support DuckDB.
> Always activate the venv first.

```bash
# Activate venv (Git Bash on Windows)
source .venv/Scripts/activate

# 1. Pull data from the Senate API (~81 API calls, ~30s)
python src/extraction/extract_senators.py

# 2. Run dbt pipeline
cd dbt_project
dbt seed --profiles-dir .      # load 30 TSE parties
dbt run  --profiles-dir .      # build 4 models
dbt test --profiles-dir .      # run 11 data quality tests
cd ..

# 3. Launch dashboard
cd dashboard
streamlit run app.py
```

Dashboard will be available at `http://localhost:8501`.

---

## Data Model

### `main_marts.dim_senador`
Grain: 1 row per senator currently in office.

| Column | Description |
|---|---|
| `senador_id` | Unique code from Senate API |
| `nome_parlamentar` | Parliamentary name |
| `nome_completo` | Full legal name |
| `sexo` | `Masculino` / `Feminino` |
| `data_nascimento` | Date of birth |
| `foto_url` | Official photo URL |
| `pagina_url` | Official page URL |
| `email` | Contact email |
| `naturalidade` | City of birth |
| `uf_naturalidade` | State of birth |
| `partido_sigla` | Party abbreviation (e.g. `PT`) |
| `partido_nome` | Full party name (from TSE seed) |
| `partido_numero_tse` | TSE registration number |
| `estado_sigla` | Represented state (UF) |
| `mandato_id` | Mandate code |
| `mandato_inicio` | Mandate start date |
| `mandato_fim` | Mandate end date (null = ongoing) |
| `descricao_participacao` | `Titular` / `Suplente` |
| `legislatura_inicio` | First legislature number |
| `legislatura_fim` | Second legislature number |
| `em_exercicio` | Boolean — currently serving |

### `main_marts.dim_partido`
Grain: 1 row per TSE-registered party.

| Column | Description |
|---|---|
| `partido_sigla` | Abbreviation |
| `partido_nome` | Full name |
| `partido_numero_tse` | TSE registration number |
| `num_senadores` | Senators currently in office |
| `tem_senador_em_exercicio` | Boolean |

---

## Dashboard Pages

### Home — Senator List (`app.py`)
- Filter by party, state (UF), and gender
- Summary metrics: total senators, parties, states, female senators
- Click any row to navigate to the senator's profile

### Senator Profile (`pages/1_Perfil_do_Senador.py`)
- Official photo
- Party, state, and mandate metrics
- Personal info: birth date, city/state of birth, email, official page
- Mandate info: legislature numbers, participation type, exercise status
- Can also be accessed directly via the sidebar selector

---

## API Source Reference

Base URL: `https://legis.senado.leg.br/dadosabertos/`
No authentication required. Append `.json` for JSON responses.

| Endpoint | Used for |
|---|---|
| `GET /senador/lista/atual.json` | Current senator list |
| `GET /senador/{code}.json` | Biographical detail |
| `GET /senador/{code}/mandatos.json` | Mandate history (plural — singular 404s) |

Key API quirks:
- `NomePartidoParlamentar` does not exist — party names come from the TSE seed
- Mandate dates are nested: `PrimeiraLegislaturaDoMandato.DataInicio` / `SegundaLegislaturaDoMandato.DataFim`
- A senator with only one mandate returns a `dict`, not a `list` — handled in extraction

---

## Project Roadmap

### Phase 1 — Senator Profiles (COMPLETE)
- [x] Extraction: senator list + detail + mandates → Parquet
- [x] dbt seed: 30 TSE parties
- [x] dbt models: `stg_senadores`, `stg_mandatos`, `dim_partido`, `dim_senador`
- [x] dbt tests: 11/11 passing (not_null, unique, accepted_values)
- [x] Streamlit: senator list with filters + senator profile page

---

### Phase 2 — Voting History (NEXT)

Enrich senator profiles with nominal vote records (how each senator voted on each bill).

**New endpoints:**
| Endpoint | Purpose |
|---|---|
| `GET /senado/votacao/lista.json?dataInicio=...` | List of voting sessions |
| `GET /senado/votacao/{id}/votos.json` | Votes per senator per session |

**New dbt models:**
```
staging/
  stg_votacoes.sql     ← voting sessions (metadata)
  stg_votos.sql        ← individual senator votes
marts/
  fact_votacao.sql     ← grain: 1 row per senator × voting session
```

**New dashboard page:**
- Voting history table per senator (yea / nay / abstain / absent)
- Party voting alignment chart
- Filter by date range or bill theme

---

### Phase 3 — Committee Memberships

Track which senators sit on which Senate committees.

**New endpoint:** `GET /senador/{code}/comissoes.json`

**New dbt models:**
```
staging/stg_comissoes.sql
marts/dim_comissao.sql
marts/bridge_senador_comissao.sql   ← many-to-many bridge table
```

**Dashboard addition:** "Committees" section on the senator profile page.

---

### Phase 4 — Automated Refresh

Keep the warehouse up-to-date without manual runs.

**Options (to decide):**
- **GitHub Actions** — run extraction + dbt on a weekly schedule (free for public repos)
- **Windows Task Scheduler** — local cron-like job (no CI/CD infrastructure needed)

Preferred approach: GitHub Actions with DuckDB committed to the repo (small file, ~few MB).

---

### Phase 5 — Public API

Expose the warehouse data via a lightweight REST API so others can build on it.

**Candidate stack:** FastAPI + `duckdb` read-only connection, deployed to Railway or Render (free tier).

**Endpoints sketch:**
```
GET /senators              → paginated senator list
GET /senators/{id}         → senator profile
GET /senators/{id}/votes   → voting history
GET /parties               → party list with senator counts
```

---

### Phase 6 — Static Website (Future)

A public-facing website for non-technical users, replacing or complementing Streamlit.

**Candidate stack:** Next.js or Astro, consuming the Phase 5 public API.

---

## Environment Notes

- **venv**: `.venv/` at repo root, created with `uv venv` + `uv pip install`
- **`dbt` in PATH = dbt-fusion 2.0** — does NOT support DuckDB. Always use the venv.
- **`streamlit` not in PATH** — use `.venv/Scripts/streamlit.exe` or activate venv first
- **DuckDB schemas**: dbt-duckdb prefixes `main_` to all custom schema names (`main_staging`, `main_marts`, `main_seeds`)
- **dbt test format** (dbt-core 1.11.6): use `data_tests:` (not `tests:`) and `arguments: values: [...]` under `accepted_values`

---

## Repository Structure

```
brazil-congress-dashboard/
├── src/
│   └── extraction/
│       ├── config.py
│       └── extract_senators.py
├── dbt_project/
│   ├── models/
│   │   ├── staging/
│   │   │   ├── sources.yml
│   │   │   ├── stg_senadores.sql
│   │   │   └── stg_mandatos.sql
│   │   └── marts/
│   │       ├── dim_partido.sql
│   │       ├── dim_senador.sql
│   │       └── schema.yml
│   ├── seeds/
│   │   └── partidos.csv
│   ├── dbt_project.yml
│   └── profiles.yml
├── dashboard/
│   ├── app.py
│   ├── queries.py
│   └── pages/
│       └── 1_Perfil_do_Senador.py
├── data/
│   ├── raw/           ← git-ignored
│   └── warehouse/     ← git-ignored
├── pyproject.toml
└── README.md
```

---

*Data source: [API de Dados Abertos do Senado Federal](https://legis.senado.leg.br/dadosabertos/)*
