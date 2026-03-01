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
