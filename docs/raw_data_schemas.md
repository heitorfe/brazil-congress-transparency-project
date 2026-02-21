# Raw Data Schemas — Brazil Senate Dashboard

Documentation of all raw data sources: API endpoints, response shapes, and
the corresponding Parquet files written to `data/raw/`.

---

## Table of Contents

1. [Senator Biographical Data](#1-senator-biographical-data)
2. [Senator Mandates](#2-senator-mandates)
3. [Voting Sessions & Votes](#3-voting-sessions--votes)

---

## 1. Senator Biographical Data

**Parquet file:** `data/raw/senadores.parquet`
**Grain:** 1 row per senator currently in office
**Extracted by:** `src/extraction/extract_senators.py`

### Endpoints

| Method | URL | Purpose |
|---|---|---|
| GET | `/senador/lista/atual.json` | List of senators currently in office |
| GET | `/senador/{code}.json` | Biographical detail per senator |

### API Response Shape — `/senador/lista/atual.json`

```json
{
  "ListaParlamentarEmExercicio": {
    "Parlamentares": {
      "Parlamentar": [
        {
          "IdentificacaoParlamentar": {
            "CodigoParlamentar": "22",
            "NomeParlamentar": "...",
            "SiglaPartidoParlamentar": "PP",
            "UfParlamentar": "SC"
          }
        }
      ]
    }
  }
}
```

### Parquet Schema

| Column | Type | Source Field | Notes |
|---|---|---|---|
| `senador_id` | String | `IdentificacaoParlamentar.CodigoParlamentar` | PK, stored as string |
| `nome_parlamentar` | String | `IdentificacaoParlamentar.NomeParlamentar` | |
| `nome_completo` | String | `IdentificacaoParlamentar.NomeCompletoParlamentar` | |
| `sexo` | String | `IdentificacaoParlamentar.SexoParlamentar` | "Masculino" / "Feminino" |
| `foto_url` | String | `IdentificacaoParlamentar.UrlFotoParlamentar` | |
| `pagina_url` | String | `IdentificacaoParlamentar.UrlPaginaParlamentar` | |
| `email` | String | `IdentificacaoParlamentar.EmailParlamentar` | Nullable |
| `partido_sigla` | String | `IdentificacaoParlamentar.SiglaPartidoParlamentar` | |
| `estado_sigla` | String | `IdentificacaoParlamentar.UfParlamentar` | |
| `data_nascimento` | String | `DadosBasicosParlamentar.DataNascimento` | Cast to date in staging |
| `naturalidade` | String | `DadosBasicosParlamentar.Naturalidade` | |
| `uf_naturalidade` | String | `DadosBasicosParlamentar.UfNaturalidade` | |

---

## 2. Senator Mandates

**Parquet file:** `data/raw/mandatos.parquet`
**Grain:** 1 row per mandate period (a senator may have multiple)
**Extracted by:** `src/extraction/extract_senators.py`

### Endpoint

| Method | URL | Purpose |
|---|---|---|
| GET | `/senador/{code}/mandatos.json` | Mandate history per senator |

> **Note:** Use the **plural** form `/mandatos.json`. The singular `/mandato.json` returns 404.

### API Response Shape

```json
{
  "MandatoParlamentar": {
    "Parlamentar": {
      "Mandatos": {
        "Mandato": [
          {
            "CodigoMandato": "12345",
            "UfParlamentar": "SC",
            "DescricaoParticipacao": "Titular",
            "PrimeiraLegislaturaDoMandato": {
              "NumeroLegislatura": "56",
              "DataInicio": "2019-02-01"
            },
            "SegundaLegislaturaDoMandato": {
              "NumeroLegislatura": "57",
              "DataFim": "2023-01-31"
            }
          }
        ]
      }
    }
  }
}
```

> **Note:** When a senator has only one mandate, the API returns a `dict`, not a list.
> The extractor handles this by wrapping singletons in a list.

### Parquet Schema

| Column | Type | Source Field | Notes |
|---|---|---|---|
| `senador_id` | String | (from parent loop) | FK → senadores.senador_id |
| `mandato_id` | String | `CodigoMandato` | PK per mandate |
| `estado_sigla` | String | `UfParlamentar` | State represented |
| `data_inicio` | String | `PrimeiraLegislaturaDoMandato.DataInicio` | Cast to date in staging |
| `data_fim` | String | `SegundaLegislaturaDoMandato.DataFim` | Null if current mandate |
| `legislatura_inicio` | String | `PrimeiraLegislaturaDoMandato.NumeroLegislatura` | |
| `legislatura_fim` | String | `SegundaLegislaturaDoMandato.NumeroLegislatura` | |
| `descricao_participacao` | String | `DescricaoParticipacao` | "Titular" / "Suplente" |

---

## 3. Voting Sessions & Votes

**Parquet files:**
- `data/raw/votacoes.parquet` — 1 row per voting session (bill × session event)
- `data/raw/votos.parquet` — 1 row per senator per voting session

**Grain:**
- `votacoes`: identified by `codigo_sessao_votacao` (unique vote event in a plenary session)
- `votos`: identified by `(codigo_sessao_votacao, codigo_parlamentar)`

**Extracted by:** `src/extraction/extract_votacoes.py`

### Endpoint

| Method | URL | Notes |
|---|---|---|
| GET | `/votacao` | New endpoint (active since 2025-03-18) |

**Query parameters:**

| Parameter | Type | Example | Notes |
|---|---|---|---|
| `dataInicio` | date | `2024-01-01` | Start of date range (YYYY-MM-DD) |
| `dataFim` | date | `2024-01-31` | End of date range (YYYY-MM-DD) |
| `casa` | string | `SF` | "SF" = Senado Federal (optional, default SF) |
| `codigoParlamentar` | int | `825` | Filter to a single senator (optional) |
| `idProcesso` | int | `7761651` | Filter to a single legislative process (optional) |
| `siglaVotoParlamentar` | string | `Sim` | Filter by vote type (optional) |
| `v` | int | `1` | API version; `v=1` wraps response in `{"votacoes":[...]}` |

> **Deprecated endpoint (DO NOT USE):** `/plenario/lista/votacao/{YYYYMMDD}/{YYYYMMDD}.json`
> — Deactivated on 2026-02-01. Replacement: `/votacao`.

### API Response Shape (date-range query, no `v` parameter)

The endpoint returns a **JSON array** at the root level. Each element is a voting session
with all senator votes nested inside.

```json
[
  {
    "codigoSessao": 384251,
    "casaSessao": "SF",
    "codigoSessaoLegislativa": 871,
    "siglaTipoSessao": "DOR",
    "numeroSessao": 5,
    "dataSessao": "2024-02-20T00:00:00",
    "idProcesso": 8294609,
    "codigoMateria": 154451,
    "identificacao": "PL 2253/2022",
    "sigla": "PL",
    "numero": "2253",
    "ano": 2022,
    "dataApresentacao": "2024-02-04T00:00:00",
    "ementa": "Altera a Lei nº 7.210...",
    "codigoSessaoVotacao": 6818,
    "codigoVotacaoSve": 4140,
    "sequencialSessao": 1,
    "votacaoSecreta": "N",
    "descricaoVotacao": "Votação nominal do Projeto de Lei nº 2.253...",
    "resultadoVotacao": "A",
    "totalVotosSim": null,
    "totalVotosNao": null,
    "totalVotosAbstencao": null,
    "informeLegislativo": {
      "id": 2178836,
      "idEvento": 10666025,
      "numeroAutuacao": 1,
      "data": "2024-02-20T20:57:21",
      "texto": "(Sessão Deliberativa Ordinária...)",
      "codigoColegiado": 1998,
      "casaColegiado": "SF",
      "siglaColegiado": "PLEN",
      "nomeColegiado": "Plenário do Senado Federal",
      "idEnteAdm": 13594,
      "casaEnteAdm": "SF",
      "siglaEnteAdm": "SEADI",
      "nomeEnteAdm": "Secretaria de Atas e Diários"
    },
    "votos": [
      {
        "codigoParlamentar": 5672,
        "nomeParlamentar": "Alan Rick",
        "sexoParlamentar": "M",
        "siglaPartidoParlamentar": "UNIÃO",
        "siglaUFParlamentar": "AC",
        "siglaVotoParlamentar": "Sim",
        "descricaoVotoParlamentar": null
      }
    ]
  }
]
```

### API Response Shape (`v=1` with `codigoParlamentar` filter)

When filtering by senator + process, `v=1` returns a **single session object** with the
`votos` array pre-filtered to that senator's vote:

```json
{
  "codigoSessao": 105398,
  "codigoSessaoVotacao": 5969,
  "resultadoVotacao": "A",
  "votos": [
    {
      "codigoParlamentar": 825,
      "nomeParlamentar": "Paulo Paim",
      "siglaVotoParlamentar": "Sim"
    }
  ]
}
```

### `votacoes.parquet` Schema (session-level)

| Column | Type | Source Field | Notes |
|---|---|---|---|
| `codigo_sessao_votacao` | Int64 | `codigoSessaoVotacao` | **PK** — unique vote event |
| `codigo_votacao_sve` | Int64 | `codigoVotacaoSve` | SVE internal code |
| `codigo_sessao` | Int64 | `codigoSessao` | Plenary session code |
| `codigo_sessao_legislativa` | Int64 | `codigoSessaoLegislativa` | Legislative session |
| `sigla_tipo_sessao` | String | `siglaTipoSessao` | e.g. "DOR" |
| `numero_sessao` | Int64 | `numeroSessao` | |
| `data_sessao` | String | `dataSessao` | ISO datetime, cast to date in staging |
| `id_processo` | Int64 | `idProcesso` | Legislative process ID |
| `codigo_materia` | Int64 | `codigoMateria` | Bill/matter code |
| `identificacao` | String | `identificacao` | e.g. "PL 2253/2022" |
| `sigla_materia` | String | `sigla` | Bill type: PL, PEC, PLP, PLS… |
| `numero_materia` | String | `numero` | Bill number |
| `ano_materia` | Int64 | `ano` | Bill year |
| `data_apresentacao` | String | `dataApresentacao` | ISO datetime, cast to date in staging |
| `ementa` | String | `ementa` | Bill summary text |
| `sequencial_sessao` | Int64 | `sequencialSessao` | Order within the plenary session |
| `votacao_secreta` | String | `votacaoSecreta` | "N" / "S" |
| `descricao_votacao` | String | `descricaoVotacao` | Free-text description |
| `resultado_votacao` | String | `resultadoVotacao` | "A"=Aprovado / "R"=Rejeitado |
| `total_votos_sim` | Int64 | `totalVotosSim` | Often null; computed in staging |
| `total_votos_nao` | Int64 | `totalVotosNao` | Often null |
| `total_votos_abstencao` | Int64 | `totalVotosAbstencao` | Often null |
| `informe_texto` | String | `informeLegislativo.texto` | Legislative record text |

### `votos.parquet` Schema (senator-level votes)

| Column | Type | Source Field | Notes |
|---|---|---|---|
| `codigo_sessao_votacao` | Int64 | `codigoSessaoVotacao` | FK → votacoes |
| `codigo_parlamentar` | Int64 | `codigoParlamentar` | FK → dim_senador (as int) |
| `nome_parlamentar` | String | `nomeParlamentar` | Denormalized for resilience |
| `sexo_parlamentar` | String | `sexoParlamentar` | "M" / "F" |
| `sigla_partido` | String | `siglaPartidoParlamentar` | Party at time of vote |
| `sigla_uf` | String | `siglaUFParlamentar` | State at time of vote |
| `sigla_voto` | String | `siglaVotoParlamentar` | See vote codes below |
| `descricao_voto` | String | `descricaoVotoParlamentar` | Nullable |

### Vote Codes (`siglaVotoParlamentar`)

| Code | Meaning |
|---|---|
| `Sim` | Yes (yea) |
| `Não` | No (nay) |
| `Abstenção` | Abstention |
| `AP` | Atividade Parlamentar — excused (parliamentary activity) |
| `LS` | Licença de Saúde — health leave |
| `MIS` | Missão Oficial — official mission |
| `P-NRV` | Presente, Não Registrou Voto — present, did not register |
| `Presidente (art. 51 RISF)` | Senate President casting vote (casting vote rule) |

### Result Codes (`resultadoVotacao`)

| Code | Meaning |
|---|---|
| `A` | Aprovado (Approved) |
| `R` | Rejeitado (Rejected) |

---

## Key Relationships

```
dim_senador.senador_id (string)  ←→  votos.codigo_parlamentar (int, cast to string for join)
dim_senador.partido_sigla        ←→  votos.sigla_partido  (current vs. historical party)
votacoes.codigo_sessao_votacao   ←→  votos.codigo_sessao_votacao
```

> **Important:** `senador_id` in our existing data is stored as a **string** (from the
> biographical API), while `codigo_parlamentar` in the voting API is an **integer**.
> Always cast one to match the other before joining. The staging models handle this.
