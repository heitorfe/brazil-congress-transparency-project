"""
Chamber of Deputies (Câmara dos Deputados) Open Data API client.

Base URL: https://dadosabertos.camara.leg.br/api/v2

Key differences from SenateApiClient:
  - All responses are wrapped in {"dados": [...], "links": [...]}
  - Pagination via "next" link in the links array
  - No .json suffix — returns JSON by default
  - No documented rate limit; uses a conservative 0.1 s delay

Usage example:
    with CamaraApiClient() as client:
        # One page
        data = client.get("/deputados", params={"idLegislatura": 57, "itens": 100})
        records = data["dados"]

        # All pages (auto-pagination)
        all_deputies = client.get_all("/deputados", params={"idLegislatura": 57})

        # Save a sample for documentation
        client.save_sample("deputados_57", all_deputies[:5])
"""

import json
import sys
import time
from pathlib import Path
from typing import Any

import httpx

if sys.stdout.encoding != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

CAMARA_BASE = "https://dadosabertos.camara.leg.br/api/v2"
SAMPLE_DIR = Path("data/api_sample")


class CamaraApiClient:
    """
    HTTP client for the Chamber of Deputies open-data API v2.

    Parameters
    ----------
    delay : float
        Seconds to sleep after each request (default 0.1 s — conservative
        for an undocumented rate limit).
    timeout : float
        HTTP request timeout in seconds.
    """

    def __init__(
        self,
        delay: float = 0.1,
        timeout: float = 60.0,
    ) -> None:
        self._delay = delay
        self._client = httpx.Client(
            timeout=timeout,
            follow_redirects=True,
            headers={"Accept": "application/json"},
        )

    # ------------------------------------------------------------------
    # Public helpers
    # ------------------------------------------------------------------

    def get(
        self,
        path: str,
        params: dict[str, Any] | None = None,
    ) -> dict:
        """
        GET one page from the Chamber API.

        Returns the full envelope dict: {"dados": [...], "links": [...]}.
        """
        url = f"{CAMARA_BASE}{path}"
        resp = self._client.get(url, params=params)
        resp.raise_for_status()
        time.sleep(self._delay)
        return resp.json()

    def get_all(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        *,
        max_pages: int = 500,
    ) -> list[dict]:
        """
        GET all pages for a paginated endpoint, following "next" links.

        Parameters
        ----------
        path : str
            Relative API path, e.g. "/deputados".
        params : dict, optional
            Initial query parameters. ``itens=100`` is added automatically
            if not already present.
        max_pages : int
            Safety cap to prevent runaway pagination (default 500 = 50,000 rows).

        Returns
        -------
        list[dict]
            All ``dados`` records from all pages combined.
        """
        if params is None:
            params = {}
        if "itens" not in params:
            params = {**params, "itens": 100}

        all_records: list[dict] = []
        # First request: use the base URL + params
        url: str | None = f"{CAMARA_BASE}{path}"
        current_params: dict | None = params
        pages_fetched = 0

        while url and pages_fetched < max_pages:
            resp = self._client.get(url, params=current_params)
            resp.raise_for_status()
            time.sleep(self._delay)

            data = resp.json()
            records = data.get("dados") or []
            if not records:
                break

            all_records.extend(records)
            pages_fetched += 1

            # Follow the "next" link; it already has all query params embedded
            url = next(
                (lnk["href"] for lnk in (data.get("links") or []) if lnk.get("rel") == "next"),
                None,
            )
            # After the first page the full URL is used; no extra params needed
            current_params = None

        return all_records

    def save_sample(
        self,
        name: str,
        data: Any,
        *,
        max_records: int = 5,
    ) -> Path:
        """Save a truncated JSON sample to data/api_sample/<name>.json."""
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

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
