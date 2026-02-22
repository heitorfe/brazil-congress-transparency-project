# Committee Data — API Endpoint Comparison

**Updated:** 2026-02-21
**Context:** `src/extraction/extract_comissoes.py` consolidation

---

## Summary

The Senate LEGIS API exposes committee (colegiado) data through several overlapping endpoints.
This document describes each endpoint, its schema, and the strategy chosen for extraction.

---

## Endpoint Inventory

### 1. `/comissao/lista/{tipo}` — Per-Type List (Legacy, Not Used)

| Property | Value |
|---|---|
| Method | `GET` |
| Auth | None |
| Suffix | `.json` |
| Types | `permanente`, `cpi`, `temporaria`, `orgaos` |
| Example | `/comissao/lista/permanente.json` |

**Response shape:**
```json
{
  "ListaBasicaComissoes": {
    "colegiado": {
      "colegiados": [
        {
          "colegiado": [
            {
              "CodigoColegiado": "38",
              "SiglaColegiado": "CAE",
              "NomeColegiado": "Comissão de Assuntos Econômicos",
              "DataInicio": "1900-01-01",
              "tipocolegiado": {
                "DescricaoTipoColegiado": "Comissão Permanente",
                "SiglaCasa": "SF"
              }
            }
          ]
        }
      ]
    }
  }
}
```

**Quirks:**
- `/comissao/lista` (no suffix) returns **400 Bad Request** — tipo suffix is mandatory
- Nested structure: `ListaBasicaComissoes → colegiado → colegiados[] → colegiado[]`
  (two levels of wrapping; `colegiados` is a list of wrapper objects, each containing a `colegiado` list)
- The `tipocolegiado` sub-object is **lowercase** despite the parent record being PascalCase
  (XML-to-JSON artifact from namespace handling)
- Keys use `Colegiado` suffix (`CodigoColegiado`, `SiglaColegiado`), NOT `Comissao` suffix
- Returns only the 4 declared tipos; does not include mixed or joint committees

**Why deprecated:** Returns a subset of committees via 4 API calls with complex nesting.
`/comissao/lista/colegiados` is a strict superset with a flat schema.

---

### 2. `/comissao/lista/colegiados` — All Active Committees ✅ Primary Source

| Property | Value |
|---|---|
| Method | `GET` |
| Auth | None |
| Redirect | `301 → /dados/ListaColegiados.json` (follow automatically) |
| Coverage | All active committees: SF, CN, CD, CMO sub-committees |

**Response shape:**
```json
{
  "ListaColegiados": {
    "Metadados": {
      "DescricaoDataSet": "Obtém a Lista geral de Colegiados EM ATIVIDADE no Congresso Nacional."
    },
    "Colegiados": {
      "Colegiado": [
        {
          "Codigo": "38",
          "Sigla": "CAE",
          "Nome": "Comissão de Assuntos Econômicos",
          "DataInicio": "1900-01-01",
          "Publica": "S",
          "CodigoTipoColegiado": "21",
          "SiglaTipoColegiado": "PERMANENTE",
          "DescricaoTipoColegiado": "Comissão Permanente",
          "SiglaCasa": "SF"
        }
      ]
    }
  }
}
```

**Field mapping to `comissoes.parquet`:**

| API Field | Parquet Column | Notes |
|---|---|---|
| `Codigo` | `codigo_comissao` | String-cast |
| `Sigla` | `sigla_comissao` | — |
| `Nome` | `nome_comissao` | — |
| `Finalidade` | `finalidade` | Optional — committee purpose/mandate |
| `SiglaCasa` | `sigla_casa` | `SF`, `CN`, `CD` |
| `CodigoTipoColegiado` | `codigo_tipo` | Integer code for committee type |
| `SiglaTipoColegiado` | `sigla_tipo` | `PERMANENTE`, `CPI`, `CCMO`, etc. |
| `DescricaoTipoColegiado` | `descricao_tipo` | Human-readable type name |
| `DataInicio` | `data_inicio` | — |
| `DataFim` | `data_fim` | Null for all currently active committees |
| `Publica` | `publica` | `"S"` → `true`, `"N"` → `false` (boolean) |

**Key differences vs. per-tipo:**
- Flat JSON — no nested wrapper objects
- Single API call for all committee types
- Includes types beyond the 4 tipos: `CCMO`, `CCMO-CN`, `ORGAO`, etc.
- Explicit `Publica` flag (indicates public visibility in Senate system)
- `CodigoTipoColegiado` enables type joins to a committee-type lookup

**Observed counts (2026-02-21):** 207 committees (202 unique after dedup)

---

### 3. `/comissao/lista/mistas` — Joint Congress Committees ✅ Augmentation Source

| Property | Value |
|---|---|
| Method | `GET` |
| Auth | None |
| Coverage | Comissões Mistas do Congresso Nacional (CN) only |

**Response shape:**
```json
{
  "ComissoesMistasCongresso": {
    "Metadados": {
      "DescricaoDataSet": "Composição das Comissões Mistas do Congresso"
    },
    "Colegiados": {
      "Colegiado": [
        {
          "CodigoColegiado": "449",
          "NomeColegiado": "Comissão Mista de Controle das Atividades de Inteligência",
          "SiglaColegiado": "CCAI",
          "Subtitulo": "(Resolução nº 2, de 2013-CN - Art. 6º da Lei nº 9.883/1999)",
          "Finalidade": "A fiscalização e o controle externos...",
          "QuantidadesMembros": {
            "Titulares": "12",
            "SenadoresTitulares": "6",
            "DeputadosTitulares": "6"
          }
        }
      ]
    }
  }
}
```

**Field mapping to `comissoes.parquet`:**

| API Field | Parquet Column | Notes |
|---|---|---|
| `CodigoColegiado` | `codigo_comissao` | Different key name vs. `colegiados` (`Codigo`) |
| `SiglaColegiado` | `sigla_comissao` | — |
| `NomeColegiado` | `nome_comissao` | — |
| `Finalidade` | `finalidade` | — |
| *(implicit)* | `sigla_casa` | Hard-coded `"CN"` |
| `QuantidadesMembros.Titulares` | `qtd_titulares` | Integer (cast from string) |
| `QuantidadesMembros.SenadoresTitulares` | `qtd_senadores_titulares` | Integer |
| `QuantidadesMembros.DeputadosTitulares` | `qtd_deputados_titulares` | Integer |

**Merge strategy:**
- For committees also present in `colegiados` (matched by `codigo_comissao`): **augment** the existing
  record with `qtd_*` member-count fields; `fonte` becomes `"colegiados+mistas"`
- For committees NOT in `colegiados`: **append** as new records; `fonte` = `"mistas"`

**Key differences vs. `colegiados`:**
- Uses `CodigoColegiado` / `SiglaColegiado` / `NomeColegiado` (Colegiado-suffixed, matching legacy endpoint)
  vs. `Codigo` / `Sigla` / `Nome` (short, from `colegiados`)
- Adds `QuantidadesMembros` — the only source for senator/deputy composition counts
- No `DataInicio`, `Publica`, or tipo codes

**Observed counts (2026-02-21):** 7 mixed committees (5 augmented existing, 2 new)

---

### 4. `/senador/{code}/comissoes` — Membership History Per Senator

| Property | Value |
|---|---|
| Method | `GET` |
| Auth | None |
| Output | `membros_comissao.parquet` (separate from `comissoes.parquet`) |

**Response shape:**
```json
{
  "MembroComissaoParlamentar": {
    "Parlamentar": {
      "MembroComissoes": {
        "Comissao": [
          {
            "IdentificacaoComissao": {
              "CodigoComissao": "38",
              "SiglaComissao": "CAE",
              "NomeComissao": "Comissão de Assuntos Econômicos",
              "SiglaCasaComissao": "SF"
            },
            "DescricaoParticipacao": "Titular",
            "DataInicio": "2023-02-01",
            "DataFim": null
          }
        ]
      }
    }
  }
}
```

**Quirks:**
- `IdentificacaoComissao.CodigoComissao` uses the **Comissao** suffix (not Colegiado)
  — these codes should match `codigo_comissao` from `comissoes.parquet`
- `Comissao` can be a dict (not list) if senator has exactly one membership — apply singleton guard
- Historical: includes all past committee assignments, not just current ones

**Observed counts (2026-02-21):** 7,251 records across 81 senators

---

## Unified Schema: `comissoes.parquet`

| Column | Type | Source | Notes |
|---|---|---|---|
| `codigo_comissao` | String | All | Primary key |
| `sigla_comissao` | String | All | Short abbreviation |
| `nome_comissao` | String | All | Full name |
| `finalidade` | String? | colegiados, mistas | Committee purpose/mandate |
| `sigla_casa` | String | All | `SF`, `CN`, `CD` |
| `codigo_tipo` | String? | colegiados only | Integer code for committee type |
| `sigla_tipo` | String | All | `PERMANENTE`, `CPI`, `MISTA`, etc. |
| `descricao_tipo` | String | All | Human-readable type |
| `data_inicio` | String | colegiados only | Null for mistas |
| `data_fim` | String? | colegiados | Null for active committees |
| `publica` | Boolean? | colegiados only | Public visibility flag |
| `qtd_titulares` | Int64? | mistas only | Total seats (senators + deputies) |
| `qtd_senadores_titulares` | Int64? | mistas only | Senate seats |
| `qtd_deputados_titulares` | Int64? | mistas only | Chamber seats |
| `fonte` | String | Computed | `colegiados`, `colegiados+mistas`, `mistas` |

---

## Decision Log

| Decision | Rationale |
|---|---|
| Replace per-tipo loop with `colegiados` | Single call, flat schema, strict superset of per-tipo data |
| Keep `mistas` as augmentation | Only source for member-count composition data |
| Merge by `codigo_comissao` | Codes are stable identifiers shared between endpoints |
| `follow_redirects=True` on httpx client | `colegiados` returns 301 → `/dados/ListaColegiados.json`; safer globally |
| `fonte` tracking column | Enables downstream filtering to distinguish augmented vs. pure records |
