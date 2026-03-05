from datetime import date
from pathlib import Path

# Resolve paths relative to this file so extractors work from any CWD
_REPO_ROOT = Path(__file__).resolve().parent.parent.parent

BASE_URL = "https://legis.senado.leg.br/dadosabertos"
RAW_DIR = _REPO_ROOT / "data" / "raw"

DEFAULT_START_YEAR = 2019
DEFAULT_START_DATE = date(2019, 2, 1)

# Chamber of Deputies (Câmara dos Deputados)
CAMARA_BASE_URL = "https://dadosabertos.camara.leg.br/api/v2"
# Legislatures to extract: 56 = 2019-2023, 57 = 2023-2027 (current)
CAMARA_DEFAULT_LEGISLATURES = [56, 57]
CAMARA_DEFAULT_START_YEAR = 2019

# Bulk CSV historical extractors (Phase 4A)
CEAPS_BULK_START_YEAR = 2008   # Senate CEAPS bulk CSV available from 2008
CEAP_CAMARA_START_YEAR = 2009  # Chamber CEAP bulk ZIP available from 2009

# TSE Electoral Data (Phase 4B) — 2018+ URL/format era only
TSE_CDN_BASE = "https://cdn.tse.jus.br/estatistica/sead/odsele"
TSE_ELECTION_YEARS = [2018, 2022, 2024]  # 2018 & 2022 = federal; 2024 = municipal
MASKED_CPF_SENTINEL = "-4"               # TSE 2024 masks all candidate CPFs as "-4"

# Portal da Transparência — federal contracts + TransfereGov amendments (Phase 4C)
TRANSPARENCIA_CDN_BASE = "https://portaldatransparencia.gov.br/download-de-dados"
TRANSPARENCIA_CONTRATOS_START_YEAR = 2023   # default recent; backfill via --skip-existing
TRANSFEREGOV_CDN_BASE = "https://portaldatransparencia.gov.br/download-de-dados"  # same base
TRANSFEREGOV_START_YEAR = 2015              # full history for emendas recipients
