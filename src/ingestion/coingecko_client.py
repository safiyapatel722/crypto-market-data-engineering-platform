# src/ingestion/coingecko_client.py

import requests
from datetime import datetime
from src.utils.logger import get_logger

logger = get_logger(__name__)

BASE_URL = "https://api.coingecko.com/api/v3"

class CoinGeckoClient:

    def __init__(self):
        self.session = requests.Session()

    def fetch_market_chart(self, coin_id: str, days: int) -> dict:
        """
        Fetch historical market chart data for a given coin.
        Returns full JSON response (no transformation).
        """
        url = f"{BASE_URL}/coins/{coin_id}/market_chart"
        params = {
            "vs_currency": "usd",
            "days": days
        }

        logger.info(f"Fetching {days} days data for {coin_id}")

        response = self.session.get(url, params=params)

        if response.status_code != 200:
            logger.error(f"API call failed: {response.status_code} - {response.text}")
            response.raise_for_status()

        logger.info(f"Successfully fetched data for {coin_id}")
        return response.json()