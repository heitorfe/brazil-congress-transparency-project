# 📊 1) Overview of Both Senate APIs

## 🧩 A. Administrative Open Data API — *adm.senado.gov.br*

This API provides **administrative data** about senators and their resource usage.

Common patterns likely include:

* `/senadores/despesas_ceaps/{year}` — CEAPS reimbursement data (senator expenses). ([Reddit][2])
* Expenses-related endpoints for different categories.
* Likely more granular budget-related data.

**Importance for transparency**

* High public value: resource usage, reimbursements, expenses.
* Useful for accountability metrics and cost-of-office dashboards.

---

## 🏛 B. Legislative Open Data API — *legis.senado.leg.br*

This API exposes **legislative processes**, including:

**Main data groups expected from the official documentation**

* Senators (bio, mandates, properties)
* Legislative proposals (matérias) and their metadata
* Voting sessions and nominal votes
* Committee info (composition, meetings, agenda)
* Plenary (sessions, ordinances, daily records) — from catalog sections. ([Senado Federal][1])

The *catalogue of open data* indicates that the Senate’s legislative data includes these structured groups: ([Senado Federal][1])

| Domain                  | Likely endpoint group                 |
| ----------------------- | ------------------------------------- |
| Senators                | senator profiles, terms, positions    |
| Proposals               | list / detailed legislative documents |
| Votes                   | sessions, roll-call votes, results    |
| Parties & composition   | blocs, party affiliations             |
| Committees              | committees, meetings, agendas         |
| Legislative interaction | authors, rapporteurs                  |

That API is OpenAPI/Swagger documented, searchable and usable without auth. ([Legis Senado][3])

---

# 🧠 2) Top-Level Data Domains (Strategic)

## 📌 A. High Public Interest *Core* (Should Be Materialized in Warehouse)

These are stable, frequently aggregated, enable metrics, rarely change structurally:

### ✔ Senators & Mandates

* Senator ID, name, party, UF (state)
* Term start/end dates
* Roles/leadership positions
* Party alignment over time

**Why important**: fundamental entity for almost all metrics.

---

### ✔ Roll-call Votes and Sessions

* Voting ID, date, session ID
* Bill ID, type, headline
* Vote results
* Individual votes (Yes/No/Abstain)

**Why important**: used for ideological scoring, attendance, voting patterns.

---

### ✔ Legislative Proposals Metadata

* Bill ID, type (PL, PEC, etc.)
* Year, subject
* Authors and rapporteurs
* Current status / stage

**Why important**: key activity indicator for productivity (bills proposed, reported).

---

### ✔ Committee Assignments & Activity

* Committee ID, type, scope
* Senator members
* Meeting dates, agendas, outcomes

**Why important**: commissions are core to legislative process.

---

### ✔ Administrative Expenses

* CEAPS (indemnity expenses)
* Official reimbursements

**Why important**: transparency of resource usage.

---

## 🟡 Medium Public Interest (Hybrid Materialization)

These domains may benefit from summary tables but fetch details live:

### ● Detailed Bill Text

* Full text tends to be large.
* Store summary (title, type, dates) in warehouse.
* Fetch body text on demand for detail screens.

---

### ● Committee Meeting Details

* Full minutes, agenda details — heavy text.
* Good to store meta, fetch full item detail live.

---

## ⚪ Lower Public Interest / Niche (Live API Only)

These are useful for exploration or drill down but not core metrics:

### ○ Administrative tables

* Salary & remuneration of non-parliamentary staff (if present)
* Historical system logs
* SAR Q&A or rarely used catalogs

---

# 🧾 3) Preliminary Data Dictionary (Conceptual)

Below is an initial mapping of **key entities** we should expect from the Swagger and associated catalog metadata. This is the foundation of your dbt models and warehouse schema.

---

## 🧍 Senator (dim_parlamentar)

* senator_id (PK)
* name
* party
* state
* birth_date (nullable)
* mandate_start
* mandate_end
* current_status
* roles (leadership roles list)

> This entity anchors voting, proposals, expenses. High frequency and highly relational.

---

## 📄 Legislative Proposal (dim_proposal)

* proposal_id (PK)
* type (PL / PEC / PLS / MPV / etc.)
* number
* year
* headline_text
* subject / theme
* authors (list)
* rapporteurs (list)

> Use as core fact dimension for rollups.

---

## 🗳 Vote Event (dim_vote_event)

* vote_id (PK)
* session_date
* session_type (plenário / comissão)
* description
* result_summary
* related_proposal_id

---

## ✍ Individual Vote (fact_vote)

* vote_id (FK)
* senator_id (FK)
* vote_option (Yes / No / Abstain / Absent)

---

## 👥 Committee (dim_committee)

* committee_id (PK)
* name
* type (permanent / temporary / CPI)
* start_date
* end_date
* materia_tramitando_count (live fetch)

---

## 💸 Expense (fact_expense_ceaps)

* senator_id (FK)
* year
* total_ceaps
* category_breakdowns (optional)

---

# 📊 4) Strategic Prioritization Matrix

This is a rough taxonomy of what should be in the warehouse vs live fetch.

| Domain                    | Warehouse | Hybrid | Live |
| ------------------------- | --------- | ------ | ---- |
| Senators                  | ✔         |        |      |
| Party history             | ✔         |        |      |
| Vote sessions             | ✔         |        |      |
| Individual votes          | ✔         |        |      |
| Proposals metadata        | ✔         |        |      |
| Bill full text            |           | ✔      |      |
| Committee composition     | ✔         |        |      |
| Committee meeting agendas |           | ✔      |      |
| Expense summaries         | ✔         |        |      |
| Expense detail receipts   |           | ✔      |      |
| Rare admin catalogs       |           |        | ⚪    |

---

# 🏆 Early Guess: Most Public-Relevant API Groups

These domains are likely to matter most for a transparency dashboard:

### ⭐ Highest Priority

1. Voting records (nominal votes)
2. Proposal metadata (bills, PECs)
3. Senator profiles & mandates
4. Committees & roles
5. Expense reimbursements (CEAPS)
6. Party & bloc membership history

---

### 🧠 Medium Priority

1. Committee meetings
2. Agenda items
3. Rapporteur performance
4. Bill text detail (for drill-downs)

---

### 📌 Lower Priority (initial)

1. Internal administrative logs
2. Staff remuneration
3. Rare lookup tables

---

# 📍 Next Steps in Exploration

To advance this work, the next analytical tasks should be:

1. Extract and list **all endpoints** from both Swagger docs (including response schema).

2. For each endpoint, annotate:

   * Path
   * Method
   * Parameters
   * JSON response schema
   * Pagination support
   * Filtering options (dates, id, etc.)
   * Data volume risk
   * Whether incremental pull is possible
   * Public interest classification

3. Define key entities and relationships explicitly (for dbt `sources.yml`).

4. Prototype queries against sample endpoints randomly to assess:

   * Structural consistency
   * Response size
   * Edge cases

---

# 📌 Supporting Context

* The Senate *Open Data Portal* provides both **administrative and legislative datasets**, often with CSV and web service options. ([Senado Federal][1])
* Committees, session votes, plenary ordinances, and other legislative metadata are all part of the catalog. ([Senado Federal][1])
* CEAPS expense data is directly available per senator per year. ([Reddit][2])

---

If you want, I can now **generate a first draft of a data catalog** that lists all expected endpoint names with placeholders for fields and parameters — ready to fill with Swagger extracted details. Want that next?

[1]: https://www12.senado.leg.br/dados-abertos?utm_source=chatgpt.com "Dados Abertos"
[2]: https://www.reddit.com/r/dataisbeautiful/comments/1f5pnif?utm_source=chatgpt.com "Total CEAPS by each state (UF) from Brazilian Senate. [OC]

➢ CEAPS: The quota for the exercise of parliamentary activity - CEAPS is the amount allocated to
reimbursement of senators' expenses incurred in the exercise of parliamentary activity.

Source: https://www12.senado.leg.br/dados-abertos"
[3]: https://legis.senado.leg.br/dadosabertos/api-docs/swagger-ui/index.html?utm_source=chatgpt.com "Swagger UI"
