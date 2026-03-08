# tests/test_coingecko_client.py

import pytest
import responses as resp
from requests.exceptions import HTTPError, Timeout, RetryError
from unittest.mock import patch

from src.ingestion.coingecko_client import CoinGeckoClient


# ── FIXTURES ──────────────────────────────────────────────────────────────────

@pytest.fixture
def client():
    """
    Creates a fresh CoinGeckoClient before each test.
    Same as sinon.stub() setup in beforeEach.
    """
    return CoinGeckoClient()


@pytest.fixture
def valid_api_response():
    """
    A realistic fake API response payload.
    CoinGecko returns [timestamp_ms, value] pairs in three arrays.
    """
    return {
        "prices":        [[1704067200000, 42000.0], [1704153600000, 43000.0]],
        "market_caps":   [[1704067200000, 820000000000.0], [1704153600000, 840000000000.0]],
        "total_volumes": [[1704067200000, 15000000000.0], [1704153600000, 16000000000.0]],
    }


# ── TEST 1 — Happy path ───────────────────────────────────────────────────────

@resp.activate
def test_fetch_market_chart_success(client, valid_api_response):
    """
    When the API returns a valid response,
    fetch_market_chart() should return the parsed dict.
    """
    # ARRANGE — register a fake URL response
    resp.add(
        method = resp.GET,
        url    = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart",
        json   = valid_api_response,
        status = 200,
    )

    # ACT
    result = client.fetch_market_chart(coin_id="bitcoin", days=7)

    # ASSERT
    assert "prices"        in result
    assert "market_caps"   in result
    assert "total_volumes" in result
    assert len(result["prices"]) == 2
    assert result["prices"][0][1] == 42000.0


# ── TEST 2 — Empty arrays (HTTP 200 but useless data) ─────────────────────────

@resp.activate
def test_fetch_raises_when_prices_empty(client):
    """
    CoinGecko sometimes returns HTTP 200 with empty arrays
    for delisted coins. We should raise ValueError not return empty data.
    """
    resp.add(
        method = resp.GET,
        url    = "https://api.coingecko.com/api/v3/coins/delisted-coin/market_chart",
        json   = {"prices": [], "market_caps": [], "total_volumes": []},
        status = 200,
    )

    with pytest.raises(ValueError, match="missing or empty"):
        client.fetch_market_chart(coin_id="delisted-coin", days=7)


# ── TEST 3 — Missing fields entirely ──────────────────────────────────────────

@resp.activate
def test_fetch_raises_when_fields_missing(client):
    """
    If the API returns a response with no recognised fields at all,
    ValueError should be raised with a clear message.
    """
    resp.add(
        method = resp.GET,
        url    = "https://api.coingecko.com/api/v3/coins/weird-coin/market_chart",
        json   = {"error": "coin not found"},
        status = 200,
    )

    with pytest.raises(ValueError, match="missing or empty"):
        client.fetch_market_chart(coin_id="weird-coin", days=7)


# ── TEST 4 — 429 retry behaviour ──────────────────────────────────────────────

@resp.activate
def test_fetch_retries_on_429_and_succeeds(client, valid_api_response):
    """
    When the first request returns 429 (rate limited),
    the retry adapter should automatically retry and succeed on attempt 2.
    """
    url = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart"

    # First call returns 429, second returns 200
    resp.add(method=resp.GET, url=url, status=429)
    resp.add(method=resp.GET, url=url, json=valid_api_response, status=200)

    # ACT — should succeed despite the first 429
    result = client.fetch_market_chart(coin_id="bitcoin", days=7)

    # ASSERT
    assert "prices" in result
    assert len(resp.calls) == 2   # confirmed: two HTTP calls were made


# ── TEST 5 — All retries exhausted ────────────────────────────────────────────

@resp.activate
def test_fetch_raises_after_all_retries_exhausted(client):
    """
    When every attempt returns 503, after all retries the client
    should raise RetryError — not hang forever or return None silently.
    """
    url = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart"

    # Register 6 failures (1 original + 5 retries)
    for _ in range(6):
        resp.add(method=resp.GET, url=url, status=503)

    with pytest.raises(RetryError):
        client.fetch_market_chart(coin_id="bitcoin", days=7)


# ── TEST 6 — Timeout ──────────────────────────────────────────────────────────

@resp.activate
def test_fetch_raises_on_timeout(client):
    """
    When the request hangs and exceeds our timeout=(5, 30),
    a Timeout exception should be raised.
    """
    url = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart"

    resp.add(
        method = resp.GET,
        url    = url,
        body   = Timeout(),   # raise this instead of returning a response
    )

    with pytest.raises(Timeout):
        client.fetch_market_chart(coin_id="bitcoin", days=7)


# ── TEST 7 — API key header injected ──────────────────────────────────────────

@resp.activate
def test_api_key_added_to_headers_when_set(valid_api_response):
    """
    When COINGECKO_API_KEY is set in the environment,
    every request should include the x-cg-pro-api-key header.
    """
    url = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart"
    resp.add(method=resp.GET, url=url, json=valid_api_response, status=200)

    with patch("src.ingestion.coingecko_client.COINGECKO_API_KEY", "test-key-123"):
        fresh_client = CoinGeckoClient()
        fresh_client.fetch_market_chart("bitcoin", 7)

    sent_headers = resp.calls[0].request.headers
    assert sent_headers.get("x-cg-pro-api-key") == "test-key-123"


# ── TEST 8 — No API key (free tier) ───────────────────────────────────────────

@resp.activate
def test_no_api_key_header_when_not_set(valid_api_response):
    """
    When no API key is set, the header should not be present at all.
    Free tier should work without any authentication header.
    """
    url = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart"
    resp.add(method=resp.GET, url=url, json=valid_api_response, status=200)

    with patch("src.ingestion.coingecko_client.COINGECKO_API_KEY", ""):
        fresh_client = CoinGeckoClient()
        fresh_client.fetch_market_chart("bitcoin", 7)

    sent_headers = resp.calls[0].request.headers
    assert "x-cg-pro-api-key" not in sent_headers


# ── TEST 9 — Parametrize: all 5 coins work ────────────────────────────────────

@pytest.mark.parametrize("coin_id", [
    "bitcoin", "ethereum", "solana", "ripple", "cardano"
])
@resp.activate
def test_all_project_coins_return_valid_response(coin_id, valid_api_response, client):
    """
    All 5 coins in our portfolio should return valid responses.
    Runs as 5 separate tests — one per coin.
    If solana fails you see: test_all_project_coins[solana] FAILED
    """
    resp.add(
        method = resp.GET,
        url    = f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart",
        json   = valid_api_response,
        status = 200,
    )

    result = client.fetch_market_chart(coin_id=coin_id, days=7)

    assert "prices"        in result
    assert "market_caps"   in result
    assert "total_volumes" in result