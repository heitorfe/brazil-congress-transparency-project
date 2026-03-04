# Brazil Congress Dashboard — Expansion Strategy
## From br-acc Analysis: New Data Sources, ETL Patterns & Incremental Load Design

**Author:** Analysis of `github.com/World-Open-Graph/br-acc` (cloned 2026-03-04, commit `v0.3.1`)
**Purpose:** Master prompt + blueprint for Phase 4+ implementation
**Stack fit:** dbt-duckdb + Polars + Parquet (NOT Neo4j — translate patterns accordingly)

---

## 1. Executive Summary

The `br-acc` project is a production Neo4j graph with **219M nodes and 97M relationships**,
covering 38 loaded Brazilian public data sources. Its ETL code is the most mature
open-source reference available for Brazilian transparency data engineering.

**What we borrow:** extractor patterns, download strategies, data quirks, column
mappings, and BRL parsing utilities.
**What we don't borrow:** Neo4j loading patterns, graph schema — we use DuckDB + dbt.

Our project already covers:
- ✅ Senate profiles, mandates, affiliations, committees, speeches
- ✅ Senate plenary votes + party whip guidance
- ✅ Senate staff & payroll
- ✅ Senate parliamentary amendments
- ✅ Chamber of Deputies: profiles, expenses, votes, proposals, deputy amendments

**Priority gaps** identified from br-acc:

| Priority | Source | Civic Value | Effort |
|---|---|---|---|
| 1 | Senate CEAPS (bulk CSV, 2008–now) | Very High | Low |
| 2 | Chamber CEAP (bulk ZIP, 2009–now) | Very High | Low |
| 3 | TSE Electoral data (candidates + donations, 2002–2024) | Very High | Medium |
| 4 | Portal da Transparência (federal contracts + amendments) | High | Medium |
| 5 | TransfereGov (emendas with recipients + convênios) | High | Medium |
| 6 | TSE Bens (candidate declared assets) | High | Low |
| 7 | TSE Filiados (party membership history) | High | Low |
| 8 | PEP-CGU (politically exposed persons list) | High | Trivial |
| 9 | TCU (audit court sanctions) | High | Trivial |
| 10 | IBAMA environmental embargoes | Medium | Trivial |

---

## 2. Incremental Load & Backfill Strategy

### 2.1 Design Principle: Watermark Table

Add a `watermarks` table to DuckDB (via dbt seed or a dedicated SQL file):

```sql
-- data/warehouse/senate.duckdb
CREATE TABLE IF NOT EXISTS main._watermarks (
    source_id     VARCHAR PRIMARY KEY,
    last_year     INTEGER,
    last_month    INTEGER,
    last_run_at   TIMESTAMP,
    rows_loaded   BIGINT,
    status        VARCHAR   -- 'ok' | 'error' | 'partial'
);
```

Python extractors read/write this table to know where to resume.
dbt models read it via `{{ source('watermarks', '_watermarks') }}` for incremental logic.

### 2.2 Strategy Per Source

| Source | Strategy | Backfill Window | dbt Materialization |
|---|---|---|---|
| CEAPS Senate (bulk CSV) | **Year-loop** skip-if-exists | 2008 → current year | incremental, unique_key=id |
| CEAP Câmara (bulk ZIP) | **Year-loop** skip-if-exists | 2009 → current year | incremental, unique_key=expense_id |
| TSE Candidatos | **Election year** (2002,2004…2024) | All years once | table (static once loaded) |
| TSE Doações | **Election year** (2002,2004…2024) | All years once | table (static once loaded) |
| TSE Bens | **Election year** | All years once | incremental, unique_key=id |
| TSE Filiados | **Full yearly bulk** | 2002 → current | incremental, unique_key=(cpf,partido,data_filiacao) |
| Portal Transparência — compras | **Monthly ZIP** | 2019-01 → now | incremental, unique_key=contrato_id |
| Portal Transparência — servidores | **Monthly ZIP** (already have via ADM API) | 2019-01 → now | incremental |
| Portal Transparência — emendas | **Yearly** | 2015 → now | incremental, unique_key=emenda_id |
| TransfereGov emendas | **Yearly bulk** | 2015 → now | incremental, unique_key=codigo_emenda |
| PEP-CGU | **Full refresh** (small, ~100K) | N/A | table |
| TCU | **Full refresh** (small, ~45K) | N/A | table |
| IBAMA embargoes | **Full refresh** (~80K) | N/A | table |

### 2.3 dbt Incremental Pattern

For all fact tables receiving year-loop data, use:

```sql
-- dbt_project/models/facts/fct_ceaps_senado.sql
{{ config(
    materialized='incremental',
    unique_key='expense_id',
    on_schema_change='sync_all_columns'
) }}

SELECT *
FROM {{ source('raw', 'ceaps_senado') }}
{% if is_incremental() %}
WHERE ano >= (SELECT COALESCE(MAX(ano), 2007) FROM {{ this }})
{% endif %}
```

For monthly sources already implemented (payroll), apply same pattern with
`data_competencia >= (SELECT MAX(data_competencia) FROM {{ this }})`.

### 2.4 Extractor Resume Pattern (from br-acc `_download_utils.py`)

All new extractors must implement:

```python
# Pattern: write .partial file during download, rename on success
partial = dest.with_suffix(dest.suffix + ".partial")
start_byte = partial.stat().st_size if partial.exists() else 0
if start_byte > 0:
    headers["Range"] = f"bytes={start_byte}-"  # HTTP range resume
```

This pattern from br-acc enables safe resume of large downloads (Câmara CEAP ZIPs
are 100–500 MB per year).

---

## 3. New Extractor Blueprints

### 3.1 Senate CEAPS Bulk (Priority 1)

**Source:** `https://www.senado.leg.br/transparencia/LAI/verba/{year}.csv`
**Format:** CSV, encoding=`latin-1`, sep=`;`
**Volume:** ~21K rows/year (2024), ~300K total 2008–2024
**Columns (per year):**

```
ANO, MES, SENADOR, TIPO_DESPESA, CNPJ_CPF, FORNECEDOR, DOCUMENTO, DATA,
DETALHAMENTO, VALOR_REEMBOLSADO
```

**Key quirks from br-acc:**
- Values in Brazilian locale: `"1.234,56"` — parse with `.replace(".", "").replace(",", ".")`
- `CNPJ_CPF` field contains both CNPJ (14 digits) and CPF (11 digits) — must discriminate
- `SENADOR` uses parliamentary name, not `codigo` — join to `dim_senador` via `nome_parlamentar`
- Some years have `skiprows=1` needed (extra header row)
- Stable expense ID: `SHA256(f"senado_{senator_name}_{date}_{supplier_doc}_{value}")[:16]`

**Target Parquet:** `data/raw/ceaps_senado_{year}.parquet` (one file per year)

**New dbt models:**
```
stg_legis__ceaps_senado.sql   → normalize BRL values, parse dates, classify CNPJ/CPF
fct_ceaps_senado.sql          → join to dim_senador via nome_parlamentar
agg_ceaps_senado_por_tipo.sql → total by senator × expense type × year
```

**Script:** `src/extraction/extract_ceaps_senado.py`
```python
# CLI: python extract_ceaps_senado.py --years 2008,2009,...,2026 --skip-existing
BASE_URL = "https://www.senado.leg.br/transparencia/LAI/verba/{year}.csv"
# Default years: range(2008, current_year + 1)
```

---

### 3.2 Chamber CEAP Bulk (Priority 2)

**Source:** `https://www.camara.leg.br/cotas/Ano-{year}.csv.zip`
**Format:** ZIP containing CSV, encoding=`utf-8-sig`, sep=`;`
**Volume:** ~610K rows/year (2024), ~5M+ total 2009–2024
**Columns (post-unzip):**

```
txNomeParlamentar, cpf, ideCadastro (= nuDeputadoId), sgUF, sgPartido,
codLegislatura, numSubCota, txtDescricao, numEspecificacaoSubCota,
txtDescricaoEspecificacao, txtFornecedor, txtCNPJCPF, txtNumero,
indTipoDocumento, datEmissao, vlrDocumento, vlrGlosa, vlrLiquido
```

**Key quirks from br-acc:**
- Encoding: `utf-8-sig` (BOM-prefixed UTF-8) — NOT latin-1 like Senate
- `vlrLiquido` is BRL locale string: `"1.234,56"`
- `cpf` field IS available in CEAP data (unlike the REST API per-deputy approach)
- Deputy ID field is `nuDeputadoId` — matches `dim_deputado.deputado_id`
- Stable expense ID: `SHA256(f"camara_{deputy_id}_{date}_{supplier_doc}_{value}")[:16]`
- ZIP contains one file: `Ano-{year}.csv` — extract, validate, move to final location

**This replaces our existing fct_despesa_deputado (REST API approach) with much richer
historical data.** The REST API only returns current legislature. The bulk CSV goes
back to 2009 and includes CPF.

**Target Parquet:** `data/raw/ceap_camara_{year}.parquet`

**New dbt models:**
```
stg_camara__ceap_bulk.sql     → normalize, classify supplier CNPJ/CPF
fct_ceap_camara_bulk.sql      → join to dim_deputado via nuDeputadoId
agg_ceap_por_tipo_deputado.sql
```

**Script:** `src/extraction/extract_ceap_camara.py`
```python
BASE_URL = "https://www.camara.leg.br/cotas/Ano-{year}.csv.zip"
# Extract ZIP → validate CSV → write Parquet
# Default years: range(2009, current_year + 1)
```

---

### 3.3 TSE Electoral Data (Priority 3)

**Sources:**
- Candidates: `https://cdn.tse.jus.br/estatistica/sead/odsele/consulta_cand/consulta_cand_{year}.zip`
- Donations: URL varies by year era (see below)

**Volume:** 7.1M Person nodes (all candidates 2002–2024), 8.2M donation relationships

**CRITICAL: TSE URL + Format Eras (from br-acc deep analysis)**

```
Donation URLs:
  2002-2010, 2016: .../prestacao_contas/prestacao_contas_{year}.zip
  2012, 2014:      .../prestacao_contas/prestacao_final_{year}.zip
  2018+:           .../prestacao_contas/prestacao_de_contas_eleitorais_candidatos_{year}.zip

Donation column name eras:
  2018+:    SQ_CANDIDATO, NR_CPF_CNPJ_DOADOR, VR_RECEITA, AA_ELEICAO (coded names)
  2010-2016: "Sequencial Candidato", "CPF/CNPJ do doador", "Valor receita" (Portuguese)
  2002-2008: SEQUENCIAL_CANDIDATO, CD_CPF_CGC/CD_CPF_CGC_DOA, VALOR_RECEITA (various)

ZIP structure inside donations:
  2002-2006: nested YYYY/Candidato/Receita/ReceitaCandidato.csv
  2008:      flat receitas_candidatos_{year}_brasil.csv
  2010:      nested candidato/UF/ReceitasCandidatos.txt
  2012-2014: flat receitas_candidatos_{year}_{UF}.txt
  2016+:     flat receitas_candidatos_{year}_{UF}.csv
```

**CRITICAL: TSE 2024 CPF masking sentinel**
```python
# TSE 2024 masks ALL candidate CPFs as "-4"
# strip_document("-4") → "4" → format_cpf → every candidate merges into one node
# Fix: use SQ_CANDIDATO (sequential ID) as primary key; CPF is bonus enrichment only
_MASKED_CPF_SENTINEL = "-4"
if raw_cpf == _MASKED_CPF_SENTINEL:
    cpf = None
```

**Column mapping for candidates (consistent across all years):**
```python
CANDIDATO_COLS = {
    "SQ_CANDIDATO": "sq_candidato",
    "NR_CPF_CANDIDATO": "cpf",
    "NM_CANDIDATO": "nome",
    "DS_CARGO": "cargo",
    "SG_UF": "uf",
    "NM_UE": "municipio",
    "ANO_ELEICAO": "ano",
    "SG_PARTIDO": "partido",
    "NR_CANDIDATO": "nr_candidato",
}
```

**Target Parquets:**
```
data/raw/tse_candidatos_{year}.parquet    (one per election year)
data/raw/tse_doacoes_{year}.parquet       (one per election year)
```

**New dbt models:**
```
stg_tse__candidatos.sql     → normalize names, format CPF, add sq_candidato as key
stg_tse__doacoes.sql        → normalize donor CNPJ/CPF, parse BRL amounts
dim_candidato.sql           → unique candidates across all election years
fct_doacao_eleitoral.sql    → (donor, candidate, amount, year) facts
agg_doacao_por_candidato.sql → total raised per candidate per election
```

**Join to existing dim_senador:**
```sql
-- sq_candidato links TSE candidate to our senator via CPF
-- This enables: "Did this senator receive donations from regulated industries?"
LEFT JOIN dim_senador s ON s.cpf = c.cpf   -- when CPF is available
```

**Script:** `src/extraction/extract_tse.py`
```python
# CLI: python extract_tse.py --years 2022 2024
# Or full backfill: --years 2002 2004 2006 2008 2010 2012 2014 2016 2018 2020 2022 2024
```

---

### 3.4 Portal da Transparência — Contracts + Emendas (Priority 4)

**Source:** `https://portaldatransparencia.gov.br/download-de-dados`

**Datasets:**
- **compras** (monthly ZIP): `{BASE_URL}/compras/{YYYYMM}`
  - Relevant file inside ZIP: `{YYYYMM}_Compras.csv` (skip ItemCompra, TermoAditivo files)
  - Columns: `Código Contratado` (CNPJ), `Nome Contratado`, `Objeto`, `Valor Inicial Compra`,
    `Nome Órgão`, `Data Assinatura Contrato`

- **emendas-parlamentares** (yearly ZIP): `{BASE_URL}/emendas-parlamentares/{year}`
  - Columns: `Nome do Autor da Emenda`, `Código do Autor da Emenda`, `Nome Ação`, `Valor Pago`

**Key quirks from br-acc:**
- Monthly ZIPs may contain multiple CSV files — filter by filename pattern
- Columns differ between monthly files (use `available = {real: pipe for real, pipe in col_map if real in df.columns}`)
- Emendas use yearly granularity (not monthly)
- `Código Contratado` is the CNPJ of the winning company — link to future CNPJ dim

**Target Parquets:**
```
data/raw/transparencia_contratos_{YYYYMM}.parquet  (per month)
data/raw/transparencia_emendas_{year}.parquet      (per year)
```

**New dbt models:**
```
stg_transp__contratos.sql       → parse CNPJ, BRL values, dates
stg_transp__emendas.sql         → normalize author codes (link to dim_senador/dim_deputado)
fct_contrato_federal.sql        → federal government contracts by organ + supplier
fct_emenda_portal_transp.sql    → portal-disclosed amendment execution
```

**Script:** `src/extraction/extract_transparencia.py`
```python
# CLI: python extract_transparencia.py --year 2025 --datasets compras,emendas
# Monthly loop for compras, yearly for emendas
# skip-existing ZIP logic included
```

---

### 3.5 TransfereGov Parliamentary Amendments (Priority 5)

**Source:** `https://portaldatransparencia.gov.br/download-de-dados/emendas-parlamentares/{year}`
(same portal, different dataset slug)

**Three CSV files inside the yearly ZIP:**
1. `EmendasParlamentares.csv` — amendment metadata + authors
2. `EmendasParlamentares_PorFavorecido.csv` — which company/person received the money
3. `EmendasParlamentares_Convenios.csv` — convênios linked to the amendment

**Columns in main file:**
```
Código da Emenda, Tipo de Emenda, Nome do Autor da Emenda, Código do Autor da Emenda,
Nome Função, Município, UF, Valor Empenhado, Valor Pago
```

**Columns in favorecidos file:**
```
Código da Emenda, Código do Favorecido (CNPJ/CPF), Tipo Favorecido
(Pessoa Jurídica/Física), Favorecido, Valor Recebido, Município Favorecido, UF Favorecido
```

**Key quirk: amendment value aggregation**
```python
# Multiple rows per Código da Emenda in the main file (one per municipality/action)
# Must GROUP BY Código da Emenda and SUM Valor Empenhado + Valor Pago
grouped = df.groupby("Código da Emenda")
value_pago = grouped["Valor Pago"].apply(lambda g: sum(_parse_brl(v) for v in g))
```

**New dbt models:**
```
stg_transferegov__emendas.sql
stg_transferegov__favorecidos.sql
stg_transferegov__convenios.sql
fct_emenda_transferegov.sql         → enriches existing dim_emenda
fct_favorecido_emenda.sql           → who received amendment money (company/person)
```

**Join to existing dim_emenda:**
```sql
-- Código da Emenda matches across TransfereGov and Portal da Transparência
-- Use this to enrich dim_emenda with recipient data
LEFT JOIN stg_transferegov__emendas t
    ON t.codigo_emenda = e.codigo_emenda
```

---

### 3.6 TSE Bens — Candidate Declared Assets (Priority 6)

**Source:** `https://cdn.tse.jus.br/estatistica/sead/odsele/bem_candidato/bem_candidato_{year}.zip`

**Volume:** ~14.3M declared assets across all election years

**Columns:**
```
SQ_CANDIDATO, NR_CPF_CANDIDATO, NM_CANDIDATO, DS_TIPO_BEM, DS_BEM, VR_BEM, ANO_ELEICAO
```

**Why it matters for our dashboard:**
- Cross-referencing: did a senator's declared wealth grow significantly between elections?
- Verify consistency: candidate declares R$50K in assets but received R$2M in donations

**Target Parquet:** `data/raw/tse_bens_{year}.parquet`

**New dbt models:**
```
stg_tse__bens.sql
fct_bem_declarado.sql        → asset facts per candidate per election
agg_patrimonio_candidato.sql → total declared wealth per candidate per election year
```

---

### 3.7 TSE Filiados — Party Membership History (Priority 7)

**Source:** `https://cdn.tse.jus.br/estatistica/sead/odsele/filiados/filiados_{PARTIDO}.zip`
(one ZIP per party)

**Volume:** ~16.5M membership records (all current registered party members)

**Columns:**
```
NUMERO_INSCRICAO, NOME_FILIADO, SIGLA_PARTIDO, NOME_PARTIDO, TIPO_FILIACAO,
DATA_FILIACAO, DATA_DESFILIACAO, SITUACAO, UF, MUNICIPIO
```

**Why it matters:**
- When was this senator/deputy in which party? — critical for political cohesion analysis
- This is the Brazilian electoral roll of party members — covers current + historical

**This enriches our existing `dim_filiacao` table** (currently sourced from the Senate API's
`/senador/{code}/filiacoes.json` which only covers sitting senators). TSE Filiados covers
ALL party members including candidates who never became senators.

**Target Parquet:** `data/raw/tse_filiados_{PARTIDO}.parquet`

**New dbt models:**
```
stg_tse__filiados.sql        → normalize names, parse dates, CPF when available
dim_filiacao_tse.sql         → extends dim_filiacao with TSE-sourced membership
```

---

### 3.8 PEP-CGU — Politically Exposed Persons (Priority 8)

**Source:** `https://portaldatransparencia.gov.br/download-de-dados/pep`
Format: yearly ZIP with CSV

**Volume:** ~133K records

**Columns:**
```
CPF_FORMATADO, NOME, SIGLA_FUNCAO, DESCRICAO_FUNCAO, NIVEL_FUNCAO,
NOME_ORGAO, DATA_INICIO_EXERCICIO, DATA_FIM_EXERCICIO,
DATA_INICIO_CARGO, DATA_FIM_CARGO
```

**Why it matters:**
- Official government list of politicians and high officials
- Use to tag `dim_senador`, `dim_deputado`, suppliers in expense data
- Cross-reference with CEAPS suppliers: is a senator paying a fellow PEP?

**Target Parquet:** `data/raw/pep_cgu.parquet`

**New dbt models:**
```
stg_cgu__pep.sql
dim_pep.sql         → politically exposed persons master list
```

---

### 3.9 TCU Audit Court Sanctions (Priority 9)

**Source:** `https://portal.tcu.gov.br/dados-abertos/` (multiple CSV files)

**Files:**
- `inabilitados-funcao-publica.csv` — barred from public office
- `licitantes-inidoneos.csv` — barred from bidding
- `resp-contas-julgadas-irregulares.csv` — irregular account judgments
- `resp-contas-julgadas-irreg-implicacao-eleitoral.csv` — electoral implication cases

**Volume:** ~45K sanction records total

**Key quirk:** CPF/CNPJ in TCU data may be partially masked — strip non-digits, validate length.

**New dbt models:**
```
stg_tcu__sancoes.sql
dim_sancao_tcu.sql          → unified TCU sanctions table
```

---

### 3.10 IBAMA Environmental Embargoes (Priority 10)

**Source:** `https://servicos.ibama.gov.br/ctf/publico/areasembargadas/ConsultaPublicaAreasEmbargadas.php`
Format: CSV download

**Volume:** ~79K embargo records

**Columns:**
```
Número do TAD, Nome do Autuado, CPF/CNPJ, Municipio, UF, Area_Ha, Data_TAD, Status
```

**Why it matters:**
- Cross-reference: do senators/deputies receive amendments benefiting IBAMA-embargoed companies?
- Environmental accountability dimension

---

## 4. Shared Utilities to Build

These are direct ports from br-acc that belong in our codebase:

### 4.1 `src/extraction/download_utils.py`

Port from `br-acc/etl/scripts/_download_utils.py`:

```python
# Key functions to port:
def download_file(url: str, dest: Path, *, timeout: int = 600) -> bool:
    """Streaming HTTP download with HTTP Range resume support.

    Writes to .partial file during download, renames on success.
    Handles 416 (already complete), 206 (partial content), and server
    that ignores Range header (restarts download cleanly).
    """

def safe_extract_zip(zip_path: Path, output_dir: Path, *, max_total_bytes=50*1024**3) -> list[Path]:
    """ZIP extraction with path traversal guard and zip bomb protection.

    Deletes corrupted ZIPs for automatic re-download.
    """

def validate_csv(path: Path, *, encoding="latin-1", sep=";") -> bool:
    """Read first 10 rows to verify encoding and column count."""
```

### 4.2 `src/extraction/brazil_utils.py`

Shared utilities for all extractors:

```python
import hashlib, re, unicodedata
from pathlib import Path

def parse_brl_value(value: str) -> float:
    """Parse Brazilian monetary string to float.

    Handles: '1.234,56' → 1234.56
    Handles: 'R$ 36.380,05' → 36380.05
    Returns 0.0 on empty/invalid.
    """
    if not value or not str(value).strip():
        return 0.0
    cleaned = re.sub(r"[R$\s]", "", str(value).strip())
    if "," in cleaned:
        cleaned = cleaned.replace(".", "").replace(",", ".")
    try:
        return float(cleaned)
    except ValueError:
        return 0.0

def normalize_name(name: str | None) -> str:
    """Uppercase, remove accents, collapse whitespace.

    IMPORTANT: br-acc uses NFKD normalization (Unicode compatibility
    decomposition) before removing combining characters.
    'José' → 'JOSE', 'Ângela' → 'ANGELA'
    """
    if not name:
        return ""
    nfkd = unicodedata.normalize("NFKD", name.strip().upper())
    ascii_name = "".join(c for c in nfkd if not unicodedata.combining(c))
    return re.sub(r"\s+", " ", ascii_name)

def strip_document(doc: str) -> str:
    """Return only digits from CPF/CNPJ string."""
    return re.sub(r"\D", "", str(doc))

def format_cpf(raw: str) -> str:
    """Format 11 digits as 'NNN.NNN.NNN-NN'."""
    digits = strip_document(raw)
    if len(digits) != 11:
        return digits
    return f"{digits[:3]}.{digits[3:6]}.{digits[6:9]}-{digits[9:]}"

def format_cnpj(raw: str) -> str:
    """Format 14 digits as 'NN.NNN.NNN/NNNN-NN'."""
    digits = strip_document(raw)
    if len(digits) != 14:
        return digits
    return f"{digits[:2]}.{digits[2:5]}.{digits[5:8]}/{digits[8:12]}-{digits[12:]}"

def stable_hash_id(raw: str, prefix: str = "") -> str:
    """Generate a 16-char stable ID from a composite key.

    Used for expense IDs, amendment IDs, etc.
    Pattern from br-acc SenadoPipeline.
    """
    full = f"{prefix}_{raw}" if prefix else raw
    return hashlib.sha256(full.encode()).hexdigest()[:16]

CAP_CONTRACT_VALUE = 10_000_000_000.0  # R$10B — cap to filter data entry errors

def cap_value(value: float | None) -> float | None:
    """Return None for values above R$10B (data entry errors in PNCP/compras).

    Preserves raw_value field for auditability.
    """
    if value is None:
        return None
    return None if value > CAP_CONTRACT_VALUE else value
```

---

## 5. New dbt Model Architecture

### 5.1 New Source Layers

```yaml
# dbt_project/models/staging/sources.yml additions

sources:
  - name: ceaps_bulk
    schema: main
    tables:
      - name: ceaps_senado_raw      # from ceaps_senado_{year}.parquet

  - name: ceap_camara_bulk
    schema: main
    tables:
      - name: ceap_camara_raw       # from ceap_camara_{year}.parquet

  - name: tse
    schema: main
    tables:
      - name: tse_candidatos_raw
      - name: tse_doacoes_raw
      - name: tse_bens_raw
      - name: tse_filiados_raw

  - name: transparencia
    schema: main
    tables:
      - name: transparencia_contratos_raw
      - name: transparencia_emendas_raw

  - name: transferegov
    schema: main
    tables:
      - name: transferegov_emendas_raw
      - name: transferegov_favorecidos_raw
      - name: transferegov_convenios_raw

  - name: cgu
    schema: main
    tables:
      - name: pep_cgu_raw

  - name: tcu
    schema: main
    tables:
      - name: tcu_sancoes_raw
```

### 5.2 New Marts Layer

```
dbt_project/models/marts/
├── dimensions/
│   ├── dim_candidato.sql           -- TSE electoral candidates (all years)
│   ├── dim_pep.sql                 -- CGU politically exposed persons
│   ├── dim_sancao_tcu.sql          -- TCU audit sanctions
│   └── dim_embargo_ibama.sql       -- IBAMA environmental embargoes
│
├── facts/
│   ├── fct_ceaps_senado.sql        -- Senator CEAPS expenses (bulk CSV, 2008–now)
│   ├── fct_ceap_camara.sql         -- Deputy CEAP expenses (bulk ZIP, 2009–now)
│   ├── fct_doacao_eleitoral.sql    -- Campaign donations (TSE, 2002–2024)
│   ├── fct_bem_declarado.sql       -- Candidate declared assets (TSE Bens)
│   ├── fct_contrato_federal.sql    -- Federal contracts (Portal Transparência)
│   ├── fct_emenda_transferegov.sql -- TransfereGov amendment execution
│   └── fct_favorecido_emenda.sql   -- Who received amendment money
│
└── aggregates/
    ├── agg_ceaps_por_senador.sql          -- Annual CEAPS total per senator
    ├── agg_ceap_por_deputado.sql          -- Annual CEAP total per deputy
    ├── agg_doacao_por_candidato.sql       -- Total donations raised per candidate
    ├── agg_patrimonio_candidato.sql       -- Declared wealth per election
    ├── agg_contratos_por_orgao.sql        -- Contract spending by government organ
    └── agg_emenda_por_favorecido.sql      -- Amendment recipients ranking
```

---

## 6. Cross-Source Analysis Enabled

Once the above sources are loaded, the following analytical queries become possible:

### 6.1 "Follow the money" — Senator expense + supplier cross-reference

```sql
-- Which senators spent CEAPS money with companies that also donated to their campaigns?
SELECT
    s.nome_parlamentar,
    c.nome AS supplier_name,
    c.cnpj,
    SUM(e.valor_reembolsado) AS total_ceaps_spent,
    SUM(d.valor) AS total_donations_received
FROM fct_ceaps_senado e
JOIN dim_senador s ON s.senador_id = e.senador_id
JOIN fct_doacao_eleitoral d ON d.cpf_candidato = s.cpf
JOIN stg_tse__doacoes td ON td.cpf_cnpj_doador = e.cpf_cnpj
GROUP BY 1, 2, 3
HAVING total_donations_received > 0
ORDER BY total_ceaps_spent DESC
```

### 6.2 Amendment beneficiary analysis

```sql
-- Which companies received the most amendment money from senators they donated to?
SELECT
    f.favorecido AS recipient,
    f.cnpj,
    SUM(f.valor_recebido) AS total_received,
    s.nome_parlamentar AS amendment_author
FROM fct_favorecido_emenda f
JOIN dim_emenda e ON e.codigo_emenda = f.codigo_emenda
JOIN dim_senador s ON s.senador_id = e.senador_id
JOIN fct_doacao_eleitoral d ON d.cpf_cnpj_doador = f.cnpj AND d.cpf_candidato = s.cpf
GROUP BY 1, 2, 4
ORDER BY total_received DESC
```

### 6.3 PEP cross-reference with expense suppliers

```sql
-- Are any CEAPS expense suppliers on the PEP list?
SELECT
    e.fornecedor,
    e.cpf_cnpj,
    p.descricao_funcao,
    p.nome_orgao,
    COUNT(*) AS expense_count,
    SUM(e.valor_reembolsado) AS total_spent
FROM fct_ceaps_senado e
JOIN dim_pep p ON strip_document(p.cpf) = strip_document(e.cpf_cnpj)
GROUP BY 1, 2, 3, 4
```

### 6.4 Embargoed company benefiting from amendments

```sql
-- Are any IBAMA-embargoed companies receiving parliamentary amendment money?
SELECT
    f.favorecido,
    f.cnpj,
    i.municipio AS embargo_municipality,
    i.area_ha,
    SUM(f.valor_recebido) AS total_amendment_received
FROM fct_favorecido_emenda f
JOIN dim_embargo_ibama i ON i.cnpj = f.cnpj
WHERE i.status = 'Ativo'
GROUP BY 1, 2, 3, 4
ORDER BY total_amendment_received DESC
```

---

## 7. Implementation Order (Recommended Phases)

### Phase 4A: Historical Bulk Expenses (2–3 days)

1. Create `src/extraction/download_utils.py` (port from br-acc)
2. Create `src/extraction/brazil_utils.py` (BRL parser, normalize_name, etc.)
3. `src/extraction/extract_ceaps_senado.py` — year-loop 2008–2026
4. `src/extraction/extract_ceap_camara.py` — year-loop 2009–2026
5. dbt models: `stg_legis__ceaps_senado`, `fct_ceaps_senado`, `stg_camara__ceap_bulk`, `fct_ceap_camara`
6. Dashboard: merge with existing CEAPS page, extend date range back to 2008

### Phase 4B: Electoral Data (3–4 days)

1. `src/extraction/extract_tse.py` — election years 2018, 2022, 2024 first (then backfill)
2. dbt models: `stg_tse__candidatos`, `stg_tse__doacoes`, `dim_candidato`, `fct_doacao_eleitoral`
3. Dashboard: new "Financiamento Eleitoral" page — donations by source, industry, amount

### Phase 4C: Federal Contracts & Amendments (2–3 days)

1. `src/extraction/extract_transparencia.py` — 2019–2025 compras + emendas
2. `src/extraction/extract_transferegov.py` — 2019–2025
3. dbt models: `fct_contrato_federal`, `fct_emenda_transferegov`, `fct_favorecido_emenda`
4. Dashboard: new "Contratos Federais" and enriched "Emendas" pages

### Phase 4D: Reference Data (1 day)

1. `src/extraction/extract_pep_cgu.py`
2. `src/extraction/extract_tcu.py`
3. `src/extraction/extract_ibama.py`
4. dbt models: `dim_pep`, `dim_sancao_tcu`, `dim_embargo_ibama`
5. Use these as enrichment joins in existing fact tables

### Phase 4E: TSE Historical (2–3 days)

1. `src/extraction/extract_tse_bens.py`
2. `src/extraction/extract_tse_filiados.py`
3. dbt models: `fct_bem_declarado`, `agg_patrimonio_candidato`
4. Dashboard: candidate wealth declaration timeline

---

## 8. Hosting Considerations

Given we're now building toward online deployment, the data model expansion requires:

### 8.1 Storage estimates

| Source | Raw Parquet | DuckDB warehouse |
|---|---|---|
| CEAPS Senate 2008–2026 | ~80 MB | ~50 MB |
| CEAP Câmara 2009–2026 | ~1.8 GB | ~1.2 GB |
| TSE Candidatos all years | ~300 MB | ~200 MB |
| TSE Doações all years | ~2.5 GB | ~1.5 GB |
| Transparência compras 2019–2025 | ~4 GB | ~2 GB |
| TransfereGov emendas 2015–2025 | ~500 MB | ~300 MB |
| PEP/TCU/IBAMA | ~50 MB | ~30 MB |
| **Total new data** | **~9 GB raw** | **~5 GB warehouse** |

### 8.2 Deployment options

**Option A: Static file hosting (simplest)**
- Store Parquet files in a cloud bucket (GCS/S3)
- DuckDB in the app reads directly from Parquet: `FROM read_parquet('gs://...')`
- Streamlit deployed on Cloud Run / Railway / Render
- Refresh: nightly GitHub Actions workflow runs extractors + dbt

**Option B: Motherduck (managed DuckDB cloud)**
- Attach MotherDuck from both extractor and dashboard
- `duckdb.connect('md:senate?saas_mode=true')`
- No need to manage file storage separately
- Free tier: 10 GB; paid: ~$25/mo for more

**Option C: Turso (libSQL — SQLite-compatible)**
- Export mart tables from DuckDB to SQLite format nightly
- Dashboard reads from embedded SQLite (fast, serverless-friendly)
- Limitation: no DuckDB-specific SQL features

**Recommendation: Option B (MotherDuck)** for the initial hosted version.
MotherDuck's free tier covers our estimated 5 GB mart size.

### 8.3 Incremental refresh automation

```yaml
# .github/workflows/refresh_data.yml
on:
  schedule:
    - cron: '0 3 * * *'   # 3am UTC daily (midnight Brasilia)

jobs:
  refresh:
    steps:
      - name: Extract new months
        run: |
          python src/extraction/extract_ceaps_senado.py --skip-existing
          python src/extraction/extract_ceap_camara.py --skip-existing
          # Monthly payroll (already existing)
          python src/extraction/extract_servidores.py --months 1

      - name: Run dbt
        run: |
          cd dbt_project
          ./../.venv/Scripts/dbt.exe run --profiles-dir .
          ./../.venv/Scripts/dbt.exe test --profiles-dir .

      - name: Update MotherDuck (if using Option B)
        run: python scripts/sync_to_motherduck.py
```

---

## 9. Key Technical Decisions (Carry Forward)

| Decision | Rationale |
|---|---|
| One Parquet per year (not one giant file) | Enables skip-existing incremental logic; year is natural partition key |
| BRL strings kept raw in Parquet, parsed in dbt staging | Matches our existing ADM API strategy; br-acc validates this approach |
| SHA256[:16] for synthetic expense IDs | Stable across re-runs; collision probability negligible for <50M expenses |
| `normalize_name()` with NFKD accent removal | Required for joining expense supplier names to TSE donor names across sources |
| TSE CPF masked sentinel check before any format/join | Avoids "ghost node" where all 2024 candidates merge into one row |
| `cap_contract_value(10B)` before any aggregation | PNCP and Transparência contain data entry errors (R$1T contracts) |
| `.partial` download files | Safe resume without file corruption on network interruption |
| ZIP traversal + bomb guard in extract | Security requirement — government ZIPs have been observed with unexpected structures |

---

## 10. References

- `br-acc` repo: `github.com/World-Open-Graph/br-acc` (analyzed at v0.3.1, 2026-03-04)
- br-acc key files ported from:
  - `etl/scripts/_download_utils.py` → our `download_utils.py`
  - `etl/src/bracc_etl/transforms/` → our `brazil_utils.py`
  - `etl/src/bracc_etl/pipelines/senado.py` → our `extract_ceaps_senado.py` blueprint
  - `etl/src/bracc_etl/pipelines/camara.py` → our `extract_ceap_camara.py` blueprint
  - `etl/scripts/download_tse.py` → our `extract_tse.py` blueprint
  - `etl/scripts/download_transparencia.py` → our `extract_transparencia.py` blueprint
  - `etl/src/bracc_etl/pipelines/transferegov.py` → our `extract_transferegov.py` blueprint
- Existing project strategic map: `docs/api_strategic_map.md`
- br-acc production metrics: 219M nodes, 97M rels (2026-03-01 snapshot)
