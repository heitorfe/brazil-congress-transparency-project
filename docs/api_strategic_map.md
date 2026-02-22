# Brazilian Senate Open Data APIs — Strategic Architecture Map

**Last updated:** 2026-02-22
**API version:** 4.0.3.56 (legis), v1 (adm)
**Purpose:** Master reference for extraction design, dbt modeling, and UX prioritization.

---

## 1. API Surface Overview

Two separate APIs serve the Senate open data ecosystem:

| API | Base URL | Auth | Format | Rate Limit |
|---|---|---|---|---|
| **LEGIS** (Legislative) | `https://legis.senado.leg.br/dadosabertos` | None | JSON/XML/CSV | 10 req/s (HTTP 429 on exceed) |
| **ADM** (Administrative) | `https://adm.senado.gov.br/adm-dadosabertos` | None | JSON/CSV | Not documented |

The LEGIS API uses path-based format selection (`.json` suffix) or Accept headers.
The ADM API is a standard REST JSON API under `/api/v1/`.

---

## 2. API Domain Map

### LEGIS API — 8 Thematic Domains

| Domain | Tag | Key Data | Status |
|---|---|---|---|
| **Parlamentar** | `Parlamentar` | Senator profiles, mandates, affiliations, committees, speeches | Active |
| **Votação** | `Votação` | Nominal plenary votes + committee votes | Active |
| **Plenário** | `Plenário` | Plenary sessions, caucus orientation, agendas, results | Active (some endpoints deprecated) |
| **Processo** | `Processo` | Legislative proposals, bill metadata, authorship, events | Active (replaces old `matéria`) |
| **Comissão** | `Comissão` | Committee list, composition, meetings | Active |
| **Composição** | `Composição` | Leadership positions, plenary leadership | Active |
| **Discurso** | `Discurso` | Stenographic notes, speech videos | Active |
| **Legislação** | `Legislação` | Enacted laws search | Active |
| **Orçamento** | `Orçamento` | Budget amendment packages | Active |
| **Matéria** | `Matéria` | Legacy bill endpoints | **DEPRECATED** (sunset dates set) |

### ADM API — 4 Thematic Domains

| Domain | Key Data | Status |
|---|---|---|
| **Senadores** | CEAPS expenses, housing allowance, office support | Active |
| **Servidores** | Staff list, remuneration, interns, retirement forecasts | Active |
| **Contratações** | Contracts, bids, companies, payments, fiscal documents | Active |
| **Supridos** | Petty cash / grant advances by year | Active |

---

## 3. Endpoint Strategic Classification Table

### LEGIS — Parlamentar (Senator Profile)

| Endpoint | Public Importance | Aggregatable | Incremental Feasible | Complexity | Class | Notes |
|---|---|---|---|---|---|---|
| `GET /senador/lista/atual` | **High** | Yes | Full snapshot | Low | **CORE** | 81 senators; full refresh weekly |
| `GET /senador/{code}` | **High** | Partial | ID-based | Low | **CORE** | Biographic detail per senator |
| `GET /senador/{code}/mandatos` | **High** | Yes | ID-based | Low | **CORE** | Term history; singleton-vs-list quirk |
| `GET /senador/{code}/filiacoes` | **High** | Yes | ID-based | Low | **CORE** | Party affiliation history (SCD Type 2) |
| `GET /senador/{code}/comissoes` | **High** | Yes | ID-based | Low | **CORE** | Committee membership timeline |
| `GET /senador/{code}/cargos` | **Medium** | Yes | ID-based | Low | **CORE** | Leadership roles held |
| `GET /senador/{code}/licencas` | **Medium** | Yes | ID-based | Low | **HYBRID** | Official absences; store dates, details live |
| `GET /senador/{code}/discursos` | **Medium** | Partial | Date-based | Medium | **HYBRID** | Speech metadata in warehouse; text live |
| `GET /senador/{code}/apartes` | **Low** | No | No | Low | **LIVE** | Interruptions during speeches; niche |
| `GET /senador/{code}/profissao` | **Low** | No | No | Low | **LIVE** | Professional background; static lookup |
| `GET /senador/{code}/historicoAcademico` | **Low** | No | No | Low | **LIVE** | Academic background; static lookup |
| `GET /senador/lista/legislatura/{leg}` | **Medium** | Yes | Full per leg. | Low | **CORE** | Historical senator lists by legislature |
| `GET /senador/afastados` | **Medium** | Yes | Full snapshot | Low | **HYBRID** | Currently absent senators |
| `GET /senador/partidos` | **Low** | No | Full snapshot | Low | **LIVE** | Party reference list (already in seed) |

### LEGIS — Votação (Voting Records)

| Endpoint | Public Importance | Aggregatable | Incremental Feasible | Complexity | Class | Notes |
|---|---|---|---|---|---|---|
| `GET /votacao` | **High** | Yes | **Date-based** | Medium | **CORE** | Plenary nominal votes; PRIMARY source. Monthly windows. |
| `GET /votacaoComissao/parlamentar/{code}` | **High** | Yes | ID-based | Medium | **CORE** | Committee votes per senator |
| `GET /votacaoComissao/materia/{sigla}/{num}/{ano}` | **Medium** | Partial | No | Medium | **HYBRID** | Committee votes per bill |
| `GET /votacaoComissao/comissao/{sigla}` | **Medium** | Yes | Date-based | Medium | **HYBRID** | Committee votes per commission |

### LEGIS — Plenário (Plenary Sessions)

| Endpoint | Public Importance | Aggregatable | Incremental Feasible | Complexity | Class | Notes |
|---|---|---|---|---|---|---|
| `GET /plenario/votacao/orientacaoBancada/{ini}/{fim}` | **High** | Yes | **Date-based** | Medium | **CORE** | Party whip guidance per vote — enables cohesion analysis |
| `GET /plenario/resultado/{data}` | **Medium** | Yes | Date-based | Low | **CORE** | Session results (quorum, bills voted) |
| `GET /plenario/resultado/mes/{data}` | **Medium** | Yes | Month-based | Low | **HYBRID** | Monthly summary |
| `GET /plenario/agenda/dia/{data}` | **Low** | No | Date-based | Low | **LIVE** | Daily agenda; operational only |
| `GET /plenario/agenda/mes/{data}` | **Low** | No | Date-based | Low | **LIVE** | Monthly agenda |
| `GET /plenario/lista/legislaturas` | **Low** | No | Full snapshot | Low | **CORE** | Legislature reference table |
| `GET /plenario/resultado/veto/{codigo}` | **Medium** | Partial | ID-based | Low | **HYBRID** | Presidential veto vote outcomes |
| `GET /plenario/lista/votacao/{ini}/{fim}` | N/A | N/A | N/A | N/A | **DEPRECATED** | Sunset 2026-02-01. Use `/votacao`. |

### LEGIS — Processo (Legislative Proposals)

| Endpoint | Public Importance | Aggregatable | Incremental Feasible | Complexity | Class | Notes |
|---|---|---|---|---|---|---|
| `GET /processo` | **High** | Yes | **Date-based** | Medium | **CORE** | Bill metadata: ID, type, ementa, author, status. 857 PL/2024 alone. |
| `GET /processo/{id}` | **Medium** | Partial | ID-based | Low | **HYBRID** | Full proposal detail; fetch on drill-down |
| `GET /processo/relatoria` | **Medium** | Yes | Date-based | Low | **CORE** | Rapporteur assignments — key accountability metric |
| `GET /processo/emenda` | **Medium** | Partial | Date-based | Low | **HYBRID** | Amendments to proposals |
| `GET /processo/documento` | **Low** | No | No | Medium | **LIVE** | Full document text; too large for warehouse |
| `GET /processo/prazo` | **Low** | No | No | Low | **LIVE** | Deadline tracking; operational |
| `GET /processo/siglas` | **Low** | No | No | Low | **LIVE** | Bill type reference (PL, PEC, PLS…) |
| `GET /processo/assuntos` | **Low** | No | No | Low | **LIVE** | Subject taxonomy |
| `GET /processo/classes` | **Low** | No | No | Low | **LIVE** | Process classification hierarchy |
| `GET /processo/entes` | **Low** | No | No | Low | **LIVE** | Legal entity reference |

### LEGIS — Comissão (Committees)

| Endpoint | Public Importance | Aggregatable | Incremental Feasible | Complexity | Class | Notes |
|---|---|---|---|---|---|---|
| `GET /comissao/lista` | **High** | Yes | Full snapshot | Low | **CORE** | Committee master list |
| `GET /comissao/{sigla}/composicao` | **High** | Yes | Full snapshot | Low | **CORE** | Current committee members |
| `GET /comissao/{sigla}/reunioes` | **Medium** | Yes | Date-based | Medium | **HYBRID** | Meeting schedule + outcomes |
| `GET /comissao/{sigla}/eventos` | **Low** | No | No | Low | **LIVE** | Event calendar |
| `GET /comissao/Mesa/atual` | **Low** | No | Full snapshot | Low | **LIVE** | Current board leadership |

### LEGIS — Composição (Bloc/Leadership)

| Endpoint | Public Importance | Aggregatable | Incremental Feasible | Complexity | Class | Notes |
|---|---|---|---|---|---|---|
| `GET /composicao/lideranca` | **High** | Yes | Full snapshot | Low | **CORE** | All party/bloc/gov leaders — 314 records |
| `GET /composicao/lideranca/data/{date}` | **Medium** | Yes | Date-based | Low | **HYBRID** | Historical leadership snapshot |
| `GET /composicao/mesa` | **Low** | No | Full snapshot | Low | **LIVE** | Current presiding table |

### LEGIS — Discurso (Speeches)

| Endpoint | Public Importance | Aggregatable | Incremental Feasible | Complexity | Class | Notes |
|---|---|---|---|---|---|---|
| `GET /plenario/lista/discursos/{ini}/{fim}` | **Medium** | Partial | Date-based | Low | **HYBRID** | Speech metadata; store summary |
| `GET /taquigrafia/notas/sessao/{id}` | **Low** | No | No | High | **LIVE** | Full stenographic text; huge |
| `GET /taquigrafia/videos/sessao/{id}` | **Low** | No | No | Low | **LIVE** | Video URLs; link on demand |
| `GET /senador/{code}/discursos` | **Medium** | Partial | Date-based | Medium | **HYBRID** | Per-senator speech history |

### LEGIS — Legislação (Laws)

| Endpoint | Public Importance | Aggregatable | Incremental Feasible | Complexity | Class | Notes |
|---|---|---|---|---|---|---|
| `GET /legislacao/pesquisa` | **Medium** | Partial | No | Medium | **LIVE** | Full-text law search; on demand |
| `GET /legislacao/{id}` | **Low** | No | No | Low | **LIVE** | Individual law detail |
| `GET /legislacao/tiposNorma` | **Low** | No | No | Low | **LIVE** | Law type reference |

### LEGIS — Orçamento (Budget)

| Endpoint | Public Importance | Aggregatable | Incremental Feasible | Complexity | Class | Notes |
|---|---|---|---|---|---|---|
| `GET /orcamento/lista` | **Medium** | Yes | Full snapshot | Low | **HYBRID** | Budget amendment packages |
| `GET /orcamento/oficios` | **Low** | No | No | Low | **LIVE** | Amendment correspondence |

### ADM API

| Endpoint | Public Importance | Aggregatable | Incremental Feasible | Complexity | Class | Notes |
|---|---|---|---|---|---|---|
| `GET /api/v1/senadores/despesas_ceaps/{ano}` | **High** | Yes | **Year-based** | Low | **CORE** | 21,431 records for 2024 alone; expense receipts per senator |
| `GET /api/v1/senadores/auxilio-moradia` | **High** | Yes | Full snapshot | Low | **CORE** | Housing allowance + official apartment usage; 86 records |
| `GET /api/v1/senadores/escritorios` | **Medium** | Yes | Full snapshot | Low | **CORE** | Senator support offices (locations) |
| `GET /api/v1/contratacoes/contratos` | **Medium** | Yes | Full snapshot | Medium | **HYBRID** | Senate service contracts |
| `GET /api/v1/contratacoes/empresas` | **Medium** | Yes | Full snapshot | Low | **HYBRID** | Contracted companies (292 records) |
| `GET /api/v1/contratacoes/licitacoes` | **Medium** | Partial | Date-based | Medium | **HYBRID** | Bidding processes |
| `GET /api/v1/servidores/servidores` | **High** | Yes | Full snapshot | Low | **CORE** | Senate staff registry; ~2.2k records; `dim_servidor` |
| `GET /api/v1/servidores/remuneracoes/{ano}/{mes}` | **High** | Yes | **Month-based** | Low | **CORE** | Staff payroll; ~12k records/month; `fct_remuneracao_servidor` |
| `GET /api/v1/servidores/pensionistas` | **High** | Yes | Full snapshot | Low | **CORE** | Pensioner registry; ~2k records; `dim_pensionista` |
| `GET /api/v1/servidores/pensionistas/remuneracoes/{ano}/{mes}` | **High** | Yes | **Month-based** | Low | **CORE** | Pensioner payroll; `fct_remuneracao_pensionista` |
| `GET /api/v1/servidores/horas-extras/{ano}/{mes}` | **Medium** | Yes | **Month-based** | Low | **CORE** | Overtime payments; ~525/month; `fct_hora_extra` |
| `GET /api/v1/supridos/{ano}` | **Low** | Partial | Year-based | Low | **LIVE** | Petty cash grants; niche |
| `GET /api/v1/contratacoes/terceirizados` | **Low** | No | No | Low | **LIVE** | Outsourced workers |

---

## 4. Recommended Warehouse Schema Expansion

Building on the existing `dim_senador`, `dim_partido`, `fact_votacao`, and `fact_voto`.

### New Dimension Tables (dbt Marts)

#### `dim_filiacao` — Party Affiliation History (SCD Type 2)

```
grain: 1 row per (senator × party membership period)
pk: (senador_id, partido_sigla, data_filiacao)

columns:
  senador_id        string  FK → dim_senador
  partido_codigo    string
  partido_sigla     string
  partido_nome      string
  data_filiacao     date
  data_desfiliacao  date     nullable (NULL = current)
  is_current        boolean  dbt-computed
```

**Why:** Enables "which party was this senator in at the time of this vote?" — essential for accurate political analysis. The current `dim_senador.partido_sigla` is a snapshot of today's affiliation only.

---

#### `dim_comissao` — Committee Master

```
grain: 1 row per committee
pk: codigo_comissao

columns:
  codigo_comissao   string  PK
  sigla_comissao    string
  nome_comissao     string
  sigla_casa        string  SF / CN
  tipo              string  permanent / temporary / CPI
  data_inicio       date
  data_fim          date    nullable
```

---

#### `dim_membro_comissao` — Committee Membership

```
grain: 1 row per (senator × committee × role period)
pk: (codigo_comissao, senador_id, data_inicio)

columns:
  codigo_comissao         string  FK → dim_comissao
  senador_id              string  FK → dim_senador
  descricao_participacao  string  Titular / Suplente / Presidente / ...
  data_inicio             date
  data_fim                date    nullable
  is_current              boolean
```

---

#### `dim_lideranca` — Leadership Positions

```
grain: 1 row per leadership record (not time-sliced — the API returns current snapshot)
pk: codigo (API-provided integer)

columns:
  codigo                        int
  casa                          string  SF / CN / CD
  sigla_tipo_unidade_lideranca  string  G / P / B (Governo/Partido/Bloco)
  descricao_tipo_unidade        string
  senador_id                    string  FK → dim_senador (cast from codigoParlamentar)
  nome_parlamentar              string
  data_designacao               date
  sigla_tipo_lideranca          string  L / V (Líder / Vice)
  descricao_tipo_lideranca      string
  partido_sigla                 string
  partido_nome                  string
```

---

#### `dim_processo` — Legislative Proposals

```
grain: 1 row per legislative process (proposal)
pk: id_processo (idProcesso in the API)

columns:
  id_processo           int64   PK (matches votacoes.id_processo)
  codigo_materia        int64
  identificacao         string  "PL 2253/2022"
  sigla_materia         string  PL / PEC / PLP / MPV / ...
  numero_materia        string
  ano_materia           int
  ementa                string
  tipo_documento        string
  data_apresentacao     date
  autoria               string  denormalized (free text from API)
  tramitando            boolean
  url_documento         string
  data_ultima_atualizacao  timestamp
```

**Join key:** `dim_processo.id_processo` = `fact_votacao.id_processo`

---

### New Fact Tables

#### `fact_orientacao_bancada` — Party Whip Guidance

```
grain: 1 row per (vote_event × party)
pk: (codigo_votacao_sve, partido)

columns:
  codigo_votacao_sve    int64   FK → fact_votacao (via codigoVotacaoSve)
  partido               string
  orientacao            string  SIM / NÃO / LIVRE / ABSTENÇÃO / OBSTRUÇÃO
  data_hora             timestamp
  descricao_votacao     string  (denormalized for query convenience)
  numero_sessao         int
  ano_sessao            int
```

**This is the most powerful analytical table.** Joined with `fact_voto`, it enables:
- Party cohesion score: `% of senators who followed party line`
- Government support rate by senator
- Opposition patterns over time

---

#### `fact_ceaps` — CEAPS Expense Reimbursements

```
grain: 1 row per expense receipt (21,431 records in 2024 alone)
pk: id (ADM API-provided)

columns:
  id                int64   PK
  senador_id        string  FK → dim_senador (cast from codSenador)
  nome_senador      string  denormalized
  ano               int
  mes               int
  tipo_despesa      string  expense category
  fornecedor        string  vendor name
  cpf_cnpj          string  vendor tax ID
  documento         string  invoice reference
  data              date
  detalhamento      string  description
  valor_reembolsado decimal
```

---

#### `fact_votacao_comissao` — Committee Votes

```
grain: 1 row per (committee_vote_event × senator)
pk: (codigo_votacao, codigo_parlamentar)

columns:
  codigo_votacao        string
  sigla_comissao        string  FK → dim_comissao
  codigo_reuniao        string
  data_hora_reuniao     timestamp
  codigo_materia_id     string  (identificacaoMateria e.g. "PL 2491/2019")
  descricao_votacao     string
  codigo_parlamentar    string  FK → dim_senador
  nome_parlamentar      string
  sigla_partido         string  at time of vote
  qualidade_voto        string  S / N / Abstenção / ...
  nome_presidente_sessao string
```

---

#### `fact_auxilio_moradia` — Housing Allowance (snapshot)

```
grain: 1 row per senator (current snapshot)
pk: senador_id (matched from nome_parlamentar)

columns:
  nome_parlamentar   string
  estado_eleito      string
  partido_eleito     string
  auxilio_moradia    boolean  receiving housing allowance?
  imovel_funcional   boolean  using official Senate apartment?
```

---

### Bridge Tables

#### `bridge_voto_orientacao` — (derived, not extracted)

Computed in dbt by joining `fact_voto` with `fact_orientacao_bancada` on vote ID and matching the senator's party at the time of the vote (via `dim_filiacao`). Produces:

```
  codigo_sessao_votacao   int64
  codigo_parlamentar      int64
  sigla_voto              string  actual vote
  orientacao_partido      string  party line at time
  seguiu_partido          boolean (sigla_voto == orientacao_partido)
```

---

## 5. Incremental Extraction Strategy

| Data | Strategy | Backfill | Risk |
|---|---|---|---|
| `dim_senador` / `dim_mandatos` | Full refresh weekly | One-time full pull | Low — 81 records |
| `dim_filiacao` | Full refresh per senator, weekly | Loop 81 senators once | Low |
| `dim_membro_comissao` | Full refresh per senator, weekly | Loop 81 senators once | Low |
| `fact_votacao` + `fact_voto` | **Monthly date window** (existing) | 2019-02-01 → today | Medium — 7+ years |
| `fact_orientacao_bancada` | **Monthly date window** | 2019 → today | Low — ~26 records/6mo |
| `dim_processo` | **Monthly date window** per sigla | By year × sigla | Medium — many bills |
| `dim_lideranca` | Full refresh weekly | N/A (no history) | Low |
| `fact_ceaps` | **Yearly** (`/despesas_ceaps/{ano}`) | 2019–today | Medium — 21k/year |
| `fact_auxilio_moradia` | Full refresh weekly | N/A (snapshot) | Low |
| `dim_comissao` | Full refresh weekly | N/A | Low |
| `fact_votacao_comissao` | **Per senator**, weekly | Loop 81 senators | Medium |
| `dim_servidor` | Full refresh weekly | N/A (snapshot) | Low — ~2.2k records |
| `dim_pensionista` | Full refresh weekly | N/A (snapshot) | Low — ~2k records |
| `fct_remuneracao_servidor` | **Monthly** (incremental merge on `data_competencia`) | 2019-01 → today (~152k/year) | Medium — ~1.8M rows (7 years) |
| `fct_remuneracao_pensionista` | **Monthly** (incremental merge) | 2019-01 → today | Low — ~200k rows total |
| `fct_hora_extra` | **Monthly** (full refresh — small) | 2019-01 → today | Low — ~75k rows total |
| `agg_pessoal_mensal` | Full refresh (derived from fct_remuneracao_servidor) | N/A | Low — ~5k rows |

---

## 6. UX-Driven Public Interest Ranking

Top 10 from a transparency / journalistic perspective:

| Rank | Domain | Why Citizens Care |
|---|---|---|
| 1 | **Nominal Votes** (`fact_voto`) | Core accountability: How did MY senator vote on X? |
| 2 | **CEAPS Expenses** (`fact_ceaps`) | Public money: Who spends the most? On what? With whom? |
| 3 | **Committee Memberships** | Where does this senator have power? |
| 4 | **Legislative Proposals** (`dim_processo`) | What bills did this senator author or oppose? |
| 5 | **Housing Allowance** | Is the senator using a taxpayer apartment? |
| 6 | **Leadership Positions** (`dim_lideranca`) | Is this senator part of the government bloc? |
| 7 | **Committee Votes** | Votes that happen before the plenary — where real bargaining occurs |
| 8 | **Government Support Rate** | What % of votes did each senator align with the current government? |

---

## 7. Data Volume & Performance Considerations

| Endpoint | Volume Risk | Mitigation |
|---|---|---|
| `/votacao` (full history) | **High** — 7 years of plenary votes, ~100k+ individual votes | Monthly windows, deduplicate on `codigo_sessao_votacao` |
| `/api/v1/senadores/despesas_ceaps/{ano}` | **Medium** — 21k records/year | Year-by-year fetch; one file per year in Parquet |
| `/processo` | **High** — hundreds of bills per month | Filter by `sigla` + `ano`; paginate if needed |
| `/votacaoComissao/parlamentar/{code}` | **Medium** — 171 records for one senator | Loop 81 senators; deduplicate by (codigo_votacao, codigo_parlamentar) |
| `/taquigrafia/notas/sessao/{id}` | **Very High** — full text per session | Never materialize; live only |
| `/processo/documento` | **Very High** — full bill text | Never materialize; link to `urlDocumento` |
| `orientacaoBancada` | **Low** — ~26 records per 6 months | Simple date window extraction |
| `/api/v1/servidores/remuneracoes/{ano}/{mes}` | **Medium** — ~12.7k records/month (2025 avg) | Monthly loop; monetary fields are BR locale strings → parse in staging |
| `/api/v1/servidores/horas-extras/{ano}/{mes}` | **Low** — ~525 records/month | Nested `horas_extras[]` daily array intentionally NOT exploded; monthly `valorTotal` only |

---

## 8. API Structural Risks

### Singleton vs List Inconsistency
- `/senador/{code}/mandatos`: returns `dict` (not list) when senator has only one mandate.
  **Mitigation:** `if isinstance(raw, dict): raw = [raw]` — already implemented.
- `/votacaoComissao/parlamentar/{code}`: nested `Votacao` follows same pattern.
  **Mitigation:** Apply same singleton guard.

### Type Inconsistencies — Integer vs String IDs
- `dim_senador.senador_id` is stored as **string** (from biographical API).
- `fact_voto.codigo_parlamentar` is an **integer** (from voting API).
- `fact_ceaps.cod_senador` is an **integer** (from ADM API).
- `dim_lideranca.codigoParlamentar` is an **integer**.
- **Rule:** Always `CAST(codigo_parlamentar AS VARCHAR)` before joining to `senador_id`.

### Deprecated Endpoints
- All `/materia/*` endpoints are deprecated and have sunset dates.
  **Replacement:** `/processo` (active, same data, cleaner schema).
- `/plenario/lista/votacao/{ini}/{fim}` — deactivated 2026-02-01.
  **Replacement:** `/votacao` (already implemented).
- `/senador/{code}/liderancas`, `/senador/{code}/autorias`, `/senador/{code}/relatorias`
  — deprecated. Use `/composicao/lideranca` and `/processo/relatoria` instead.

### Brazilian Locale Decimal Format (ADM API)
- The ADM API returns **all monetary values as locale-formatted strings**, not numbers.
  Example: `"36.380,05"` (`.` = thousands separator, `,` = decimal separator).
- DuckDB `CAST(... AS DECIMAL)` will fail on these strings with:
  `Conversion Error: Could not convert string "36.380,05" to DECIMAL(12,2)`
- **Strategy adopted:** Keep raw strings in Parquet (bronze = exact copy of API).
  Parse in dbt staging using `TRY_CAST(REPLACE(REPLACE(col::varchar, '.', ''), ',', '.') AS DECIMAL(12,2))`.
- Applies to: all monetary columns in `stg_adm__remuneracoes_servidores`,
  `stg_adm__remuneracoes_pensionistas`, and `stg_adm__horas_extras`.
- `TRY_CAST` (not `CAST`) is intentional — returns NULL for missing/malformed values instead of erroring.

### Encoding
- API returns UTF-8. Python on Windows defaults to cp1252.
  **Mitigation:** Always `sys.stdout.reconfigure(encoding='utf-8')` in scripts.

### Empty Responses
- Some endpoints return HTTP 200 with an empty body for date ranges with no data.
  **Mitigation:** Guard with `if r.content:` before `r.json()`.

### Date Format Inconsistencies
- Plenary votes (`/votacao`): `dataSessao` is ISO datetime `2024-02-20T00:00:00`.
- Caucus orientation: `dataInicioVotacao` is `"20/02/2024 19:56:40"` (DD/MM/YYYY HH:MM:SS).
- Process dates: `dataApresentacao` is `"2024-01-02"` (ISO date).
  **Rule:** Always cast to `date` in dbt staging using appropriate `strptime` patterns.

---

## 9. Final Architecture Recommendation

### Minimal Viable Warehouse (Phase 2)

Add in priority order:

```
Phase 2A — Political Analysis Layer (votes + party)
  ✓ fact_votacao (done)
  ✓ fact_voto (done)
  → fact_orientacao_bancada    (new — party whip guidance)
  → dim_filiacao               (new — party history SCD2)
  → bridge_voto_orientacao     (derived — cohesion scoring)

Phase 2B — Senator Profile Enrichment
  → dim_membro_comissao        (committee assignments)
  → dim_comissao               (committee master)
  → dim_lideranca              (leadership positions)

Phase 2C — Legislative Activity
  → dim_processo               (bill metadata)
  → fact_votacao_comissao      (committee votes)

Phase 2D — Financial Transparency
  → fact_ceaps                 (CEAPS expenses per receipt)
  → fact_auxilio_moradia       (housing allowance snapshot)

Phase 2E — Staff & Payroll Transparency  ✅ COMPLETE (2026-02-22)
  ✓ dim_servidor               (staff registry snapshot)
  ✓ dim_pensionista            (pensioner registry snapshot)
  ✓ fct_remuneracao_servidor   (staff payroll, 2025 backfill)
  ✓ fct_remuneracao_pensionista (pensioner payroll, 2025 backfill)
  ✓ fct_hora_extra             (overtime, 2025 backfill)
  ✓ agg_pessoal_mensal         (monthly summary by vinculo × lotacao)
  ✓ Dashboard page: Transparência de Pessoal (4 tabs)
  Note: Full 2019–present backfill requires re-running extract_servidores.py --start-year 2019
```

### Hybrid Fetch Strategy

For the Streamlit dashboard, implement two-tier data access:

1. **Warehouse tier** (DuckDB): All fact/dim tables above. Used for:
   - Aggregate metrics (total votes, expense totals, cohesion scores)
   - Filtering + searching
   - Charts and trend views

2. **Live API tier** (httpx, using `SenateApiClient`): Fetched on drill-down:
   - Full bill text (`urlDocumento` link + `/processo/{id}`)
   - Speech transcripts (`/taquigrafia/notas/sessao/{id}`)
   - Real-time committee agenda (`/comissao/{sigla}/reunioes`)
   - Senator's profile image (already a URL in `dim_senador.foto_url`)

### Caching Strategy for Streamlit

```python
@st.cache_data(ttl=3600)       # 1-hour cache for live API calls
def fetch_processo_detail(id): ...

@st.cache_data(ttl=86400)      # 24-hour cache for daily aggregates
def get_expense_summary(): ...

# DuckDB queries: no Streamlit cache needed — DuckDB is fast enough
```

### Key Standardization Rules

| Rule | Rationale |
|---|---|
| All IDs stored as `VARCHAR` in DuckDB | Prevents int/str join failures across API sources |
| Dates cast to `DATE` in staging layer | APIs return ISO datetime strings with varying precision |
| Party affiliation resolved at vote time | Use `dim_filiacao` to get senator's party on `data_sessao`, not current party |
| CEAPS `cod_senador` cast to string for join | Matches `dim_senador.senador_id` |
| `orientacaoBancada` uses `codigo_votacao_sve` as FK to `fact_votacao` | Both APIs use this SVE code as the stable vote identifier |

---

## 10. Key Relationships Diagram

```
dim_senador
  └──< dim_mandatos          (senador_id)
  └──< dim_filiacao          (senador_id)  ← resolves party-at-vote-time
  └──< dim_membro_comissao   (senador_id)
  └──< dim_lideranca         (senador_id, cast from int)
  └──< fact_ceaps            (senador_id, cast from cod_senador int)
  └──< fact_auxilio_moradia  (matched via nome_parlamentar — no ID in ADM)
  └──< fact_voto             (senador_id = CAST(codigo_parlamentar AS VARCHAR))
       └──> fact_votacao     (codigo_sessao_votacao)
            └──> dim_processo (id_processo)
            └──> fact_orientacao_bancada (codigo_votacao_sve)

dim_comissao
  └──< dim_membro_comissao   (codigo_comissao)
  └──< fact_votacao_comissao (sigla_comissao)

bridge_voto_orientacao (derived, not extracted)
  = fact_voto JOIN fact_orientacao_bancada JOIN dim_filiacao
  → enables: did_follow_party_line (boolean), cohesion metrics
```

---

## 11. Sample Data Files

All samples are saved in `data/api_sample/`:

| File | Source Endpoint | Records |
|---|---|---|
| `senador_lista_atual.json` | `/senador/lista/atual` | 2 of 81 |
| `senador_5672_filiacoes.json` | `/senador/5672/filiacoes` | Full |
| `senador_5672_comissoes.json` | `/senador/5672/comissoes` | Full |
| `senador_5672_cargos.json` | `/senador/5672/cargos` | Full |
| `composicao_lideranca.json` | `/composicao/lideranca` | 5 of 314 |
| `processo_PL_2024.json` | `/processo?sigla=PL&ano=2024` | 3 of 857 |
| `orientacao_bancada_2024H1.json` | `/plenario/votacao/orientacaoBancada/20240101/20240630` | 3 of 26 |
| `votacaoComissao_senador_5672.json` | `/votacaoComissao/parlamentar/5672` | 3 of 171 |
| `ceaps_2024.json` | ADM `/api/v1/senadores/despesas_ceaps/2024` | 5 of 21,431 |
| `auxilio_moradia.json` | ADM `/api/v1/senadores/auxilio-moradia` | 5 of 86 |
| `contratacoes_empresas.json` | ADM `/api/v1/contratacoes/empresas` | 3 of 292 |
| `despesas_ceaps_2026.json` | ADM `/api/v1/senadores/despesas_ceaps/2026` | Pre-existing |
| `empresas_contratadas.json` | ADM `/api/v1/contratacoes/empresas` | Pre-existing |
| `servidores.json` | ADM `/api/v1/servidores/servidores` | 5 of ~2.2k |
| `pensionistas.json` | ADM `/api/v1/servidores/pensionistas` | 5 of ~2k |
| `horas_extras.json` | ADM `/api/v1/servidores/horas-extras/{ano}/{mes}` | 5 of ~525/month |

---

*This document was produced from live API exploration on 2026-02-21. Update after each major extraction sprint.*
