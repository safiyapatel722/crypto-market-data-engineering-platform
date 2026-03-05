# src/ingestion/coingecko_client.py

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from configs.settings import COINGECKO_API_KEY
from src.utils.logger import get_logger

logger = get_logger(__name__)

BASE_URL = "https://api.coingecko.com/api/v3"


class CoinGeckoClient:
    """
    HTTP client for the CoinGecko v3 API.

    Fixes applied vs original:
      - Retry adapter with exponential backoff (handles 429s and 5xx automatically)
      - Connect + read timeouts on every request (no more hung pipelines)
      - Response structure validation before returning data
      - Lazy % logging instead of eager f-strings
      - Optional Pro API key support via COINGECKO_API_KEY env var
    """

    def __init__(self):
        self.session = self._build_session()

    # ── Public methods ─────────────────────────────────────────────────────────

    def fetch_market_chart(self, coin_id: str, days: int) -> dict:
        """
        Fetch historical market chart data for a given coin.

        Args:
            coin_id: CoinGecko coin ID e.g. "bitcoin", "ethereum".
            days:    Number of days of history to fetch.

        Returns:
            Dict with keys: prices, market_caps, total_volumes.
            Each value is a list of [timestamp_ms, value] pairs.

        Raises:
            requests.HTTPError: Non-2xx after all retries exhausted.
            ValueError:         Response missing required fields.
            requests.Timeout:   Request exceeded timeout limits.
        """
        url    = f"{BASE_URL}/coins/{coin_id}/market_chart"
        params = {"vs_currency": "usd", "days": days}

        # FIX: lazy % logging — f-strings always evaluate even when the log
        # level means the message is never printed (wasted CPU on hot loops).
        logger.info("Fetching %d days of market chart for '%s'", days, coin_id)

        response = self._get(url, params)
        data     = response.json()

        # Validate before returning — CoinGecko can return HTTP 200 with empty
        # arrays for delisted or low-volume coins. Without this check, the
        # pipeline fails deep in SQL with a cryptic error instead of here.
        self._validate_market_chart(data, coin_id)

        logger.info(
            "Fetched '%s' — %d price points, %d volume points",
            coin_id,
            len(data["prices"]),
            len(data["total_volumes"]),
        )
        return data

    # ── Private helpers ────────────────────────────────────────────────────────

    def _build_session(self) -> requests.Session:
        """
        Build a requests.Session with a retry adapter and optional API key header.

        Retry strategy:
          - Up to 5 retries on 429 (rate limit) and 5xx server errors.
          - Exponential backoff: 2s → 4s → 8s → 16s → 32s between retries.
          - Respects CoinGecko's Retry-After header on 429 responses.
        """
        session = requests.Session()

        # FIX: Retry adapter — previously a single 429 or 503 would crash the
        # entire pipeline run with no recovery. Now it retries automatically.
        retry_strategy = Retry(
            total                      = 5,
            backoff_factor             = 2,   # 2s, 4s, 8s, 16s, 32s
            status_forcelist           = [429, 500, 502, 503, 504],
            allowed_methods            = ["GET"],
            respect_retry_after_header = True,
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://",  adapter)

        # Optional: CoinGecko Pro API key for higher rate limits.
        # Free tier works fine without it.
        if COINGECKO_API_KEY:
            session.headers.update({"x-cg-pro-api-key": COINGECKO_API_KEY})
            logger.info("CoinGecko Pro API key loaded.")
        else:
            logger.info("No COINGECKO_API_KEY set — using free tier.")

        return session

    def _get(self, url: str, params: dict) -> requests.Response:
        """
        Execute a GET request with connect + read timeouts.

        FIX: Without a timeout, a hung connection blocks the entire pipeline
        process indefinitely — no error, no recovery, just frozen.
        (5, 30) = 5 seconds to establish connection, 30 seconds to read body.
        """
        response = self.session.get(url, params=params, timeout=(5, 30))

        if not response.ok:
            logger.error(
                "API call failed: %d %s — URL: %s",
                response.status_code, response.reason, url,
            )
            response.raise_for_status()

        return response

    def _validate_market_chart(self, data: dict, coin_id: str) -> None:
        """
        Confirm all required arrays are present and non-empty.
        Raises ValueError with a clear message so the pipeline can log and
        skip this coin rather than failing silently downstream.
        """
        required = ["prices", "market_caps", "total_volumes"]
        missing  = [k for k in required if not data.get(k)]

        if missing:
            raise ValueError(
                f"CoinGecko response for '{coin_id}' is missing or empty "
                f"fields: {missing}. The coin may be delisted or have no "
                f"trading data for the requested period."
            )