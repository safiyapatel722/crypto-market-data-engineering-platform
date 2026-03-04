
from src.ingestion.coingecko_client import CoinGeckoClient

client = CoinGeckoClient()

data = client.fetch_market_chart("bitcoin", 2)

print(data.keys())