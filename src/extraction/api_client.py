"""
Generic Senate API client for Brazil Senate Open Data APIs.

Two base APIs:
  LEGIS  — https://legis.senado.leg.br/dadosabertos   (legislative data)
  ADM    — https://adm.senado.gov.br/adm-dadosabertos (administrative data)

Rate limits:
  LEGIS: 10 req/s max → use 0.15s delay between requests.
  ADM:   no documented limit, but use conservative 0.3s delay.

Usage example:
    client = SenateApiClient()
    # Fetch JSON from LEGIS
    data = client.get_legis("/senador/lista/atual")
    # Fetch JSON from ADM
    ceaps = client.get_adm("/api/v1/senadores/despesas_ceaps/2024")
    # Save sample for documentation / debugging
    client.save_sample("ceaps_2024", ceaps)
"""

import json
import sys
import time
from pathlib import Path
from typing import Any

import httpx

if sys.stdout.encoding != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

LEGIS_BASE = "https://legis.senado.leg.br/dadosabertos"
ADM_BASE = "https://adm.senado.gov.br/adm-dadosabertos"

SAMPLE_DIR = Path("data/api_sample")


class SenateApiClient:
    """
    Thin wrapper around httpx for both Senate open-data APIs.

    Parameters
    ----------
    legis_delay : float
        Seconds to sleep after each LEGIS request (default 0.15s — well under
        the 10 req/s cap).
    adm_delay : float
        Seconds to sleep after each ADM request (default 0.3s).
    timeout : float
        HTTP request timeout in seconds.
    """

    def __init__(
        self,
        legis_delay: float = 0.15,
        adm_delay: float = 0.30,
        timeout: float = 60.0,
    ) -> None:
        self._legis_delay = legis_delay
        self._adm_delay = adm_delay
        self._client = httpx.Client(timeout=timeout)

    # ------------------------------------------------------------------
    # Public helpers
    # ------------------------------------------------------------------

    def get_legis(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        *,
        suffix: str = ".json",
    ) -> Any:
        """
        GET from the Legislative API (legis.senado.leg.br/dadosabertos).

        Parameters
        ----------
        path : str
            Relative path, e.g. "/senador/lista/atual" or "/votacao".
            The ``.json`` suffix is appended automatically unless you pass
            ``suffix=""``.
        params : dict, optional
            Query string parameters.
        suffix : str
            URL suffix, default ``.json``. Pass ``""`` for endpoints that
            don't use it (e.g. ``/votacao`` uses query params instead).

        Returns
        -------
        Parsed JSON (dict or list).
        """
        url = f"{LEGIS_BASE}{path}{suffix}"
        print(url)
        return self._get(url, params, delay=self._legis_delay)

    def get_adm(
        self,
        path: str,
        params: dict[str, Any] | None = None,
    ) -> Any:
        """
        GET from the Administrative API (adm.senado.gov.br/adm-dadosabertos).

        Parameters
        ----------
        path : str
            Relative path, e.g. "/api/v1/senadores/despesas_ceaps/2024".
        params : dict, optional
            Query string parameters.

        Returns
        -------
        Parsed JSON (dict or list).
        """
        url = f"{ADM_BASE}{path}"
        return self._get(url, params, delay=self._adm_delay)

    def save_sample(
        self,
        name: str,
        data: Any,
        *,
        max_records: int = 5,
    ) -> Path:
        """
        Save a truncated JSON sample to ``data/api_sample/<name>.json``.

        Parameters
        ----------
        name : str
            Filename stem (without extension).
        data : Any
            The parsed API response.
        max_records : int
            If data is a list, only the first ``max_records`` items are saved.

        Returns
        -------
        Path to the saved file.
        """
        SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
        sample = data[:max_records] if isinstance(data, list) else data
        out = SAMPLE_DIR / f"{name}.json"
        out.write_text(
            json.dumps(sample, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        print(f"  saved sample → {out}")
        return out

    def close(self) -> None:
        self._client.close()

    # context-manager support
    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _get(
        self,
        url: str,
        params: dict[str, Any] | None,
        *,
        delay: float,
    ) -> Any:
        resp = self._client.get(url, params=params)
        resp.raise_for_status()
        time.sleep(delay)
        return resp.json()
