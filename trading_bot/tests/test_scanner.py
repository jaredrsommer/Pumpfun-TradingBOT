import asyncio
import unittest
from unittest.mock import AsyncMock, patch
import logging
from datetime import datetime, timedelta
import json

import aiohttp

from trading_bot.scanner import TokenScanner
# Import config names that will be patched at the module level of scanner
# Also import the actual API endpoint string values for the mock side_effect
from trading_bot.config import (
    STATIC_RUGCHECK_JWT,
    RUGCHECK_AUTH_SOLANA_PRIVATE_KEY,
    RUGCHECK_AUTH_WALLET_PUBLIC_KEY,
    RUGCHECK_SCORE_THRESHOLD,
    RUGCHECK_CRITICAL_RISK_NAMES,
    TARGET_MARKET_CAP_TO_SCAN,
    MAX_MARKET_CAP,
    MAX_TOKEN_AGE_HOURS,
    MIN_LIQUIDITY,
    MIN_TRANSACTIONS,
    MIN_BUY_SELL_RATIO,
    VOLUME_SPIKE_THRESHOLD,
    MIN_HOLDER_COUNT,
    FILTER_FOR_PUMPFUN_ONLY,
    PUMPFUN_ADDRESS_SUFFIX,
    DEXSCREENER_TOKEN_PROFILES_API as DEXSCREENER_TOKEN_PROFILES_API_VAL, # Actual value for mock
    DEXSCREENER_SEARCH_API as DEXSCREENER_SEARCH_API_VAL, # Actual value for mock
    RUGCHECK_API_ENDPOINT as RUGCHECK_API_ENDPOINT_VAL # Actual value for mock
)

logging.disable(logging.CRITICAL) # Disable logging for tests

# --- Mock Data Samples ---
MOCK_TOKEN_MINT_ADDR_1 = "mintAddr1_valid"
MOCK_TOKEN_SYMBOL_1 = "VALID1"
MOCK_PAIR_ADDR_1 = "pairAddr1_valid_vs_SOL"

MOCK_TOKEN_PROFILE_LIST_VALID = [ # This is the list itself
    {
        "tokenAddress": MOCK_TOKEN_MINT_ADDR_1,
        "symbol": MOCK_TOKEN_SYMBOL_1,
        "name": "Valid Token 1",
    }
]

MOCK_DETAILED_PAIR_DATA_VALID = { # This is a single pair object
    "schemaVersion": "1.0.0",
    "pairAddress": MOCK_PAIR_ADDR_1,
    "baseToken": {"address": MOCK_TOKEN_MINT_ADDR_1, "symbol": MOCK_TOKEN_SYMBOL_1},
    "quoteToken": {"symbol": "SOL"},
    "fdv": 50000, # Market Cap
    "pairCreatedAt": (datetime.now() - timedelta(hours=1)).timestamp() * 1000, # Age
    "liquidity": {"usd": 60000}, # Liquidity
    "txns": {"h1": {"buys": 100, "sells": 50}}, # Transactions & Buy/Sell Ratio
    "volume": {"h1": 10000, "h24": 100000}, # Volume Spike
    "priceChange": {"h1": 5}, # Price Change
}

MOCK_PAIR_SEARCH_RESPONSE_VALID = {"pairs": [MOCK_DETAILED_PAIR_DATA_VALID]}

MOCK_TOKEN_MINT_ADDR_2_FAILS_METRICS = "mintAddr2_fails"
MOCK_TOKEN_SYMBOL_2_FAILS_METRICS = "FAILMCAP"
MOCK_DETAILED_PAIR_DATA_FAILS_METRICS = {
    "pairAddress": "pairAddr2_fails_mcap",
    "baseToken": {"address": MOCK_TOKEN_MINT_ADDR_2_FAILS_METRICS, "symbol": MOCK_TOKEN_SYMBOL_2_FAILS_METRICS},
    "quoteToken": {"symbol": "SOL"},
    "fdv": 100, # Fails TARGET_MARKET_CAP_TO_SCAN
    "pairCreatedAt": (datetime.now() - timedelta(hours=1)).timestamp() * 1000,
    "liquidity": {"usd": 60000},
    "txns": {"h1": {"buys": 100, "sells": 50}},
    "volume": {"h1": 10000, "h24": 100000},
    "priceChange": {"h1": 5},
}
MOCK_PAIR_SEARCH_RESPONSE_FAILS_METRICS = {"pairs": [MOCK_DETAILED_PAIR_DATA_FAILS_METRICS]}

MOCK_EMPTY_PAIR_SEARCH_RESPONSE = {"pairs": []}

MOCK_RUGCHECK_RESPONSE_SAFE = {"scoreNormalised": 5, "rugged": False, "risks": []}
MOCK_RUGCHECK_RESPONSE_UNSAFE = {"scoreNormalised": 50, "rugged": True, "risks": [{"name": "Rugpull", "level": "critical"}]}


# --- Intelligent Mock for aiohttp.ClientSession.get ---
async def mock_get_side_effect(url, params=None, headers=None, **kwargs):
    mock_resp = AsyncMock(spec=aiohttp.ClientResponse)
    mock_resp.status = 200
    # Important: __aenter__ and __aexit__ must be awaitable or return an awaitable if they do something async
    mock_resp.__aenter__.return_value = mock_resp
    mock_resp.__aexit__ = AsyncMock(return_value=None) # Ensure it's awaitable

    # Token Profiles API (Phase 1)
    if DEXSCREENER_TOKEN_PROFILES_API_VAL == url:
        mock_resp.json = AsyncMock(return_value=MOCK_TOKEN_PROFILE_LIST_VALID)
    # Pair Search API (Phase 2)
    elif url.startswith(DEXSCREENER_SEARCH_API_VAL) and "/search" in url:
        query = params.get('q', '')
        if query == f"{MOCK_TOKEN_SYMBOL_1}/SOL" or query == MOCK_TOKEN_MINT_ADDR_1:
            mock_resp.json = AsyncMock(return_value=MOCK_PAIR_SEARCH_RESPONSE_VALID)
        elif query == f"{MOCK_TOKEN_SYMBOL_2_FAILS_METRICS}/SOL" or query == MOCK_TOKEN_MINT_ADDR_2_FAILS_METRICS:
             mock_resp.json = AsyncMock(return_value=MOCK_PAIR_SEARCH_RESPONSE_FAILS_METRICS)
        # Pumpfun token that matches suffix
        elif query == "PUMP1/SOL" or query == "testtoken1pump":
            pump_pair_data = MOCK_DETAILED_PAIR_DATA_VALID.copy()
            pump_pair_data["baseToken"]["address"] = "testtoken1pump"
            pump_pair_data["baseToken"]["symbol"] = "PUMP1"
            pump_pair_data["pairAddress"] = "pair_for_pump_token"
            mock_resp.json = AsyncMock(return_value={"pairs": [pump_pair_data]})
        else:
            mock_resp.json = AsyncMock(return_value=MOCK_EMPTY_PAIR_SEARCH_RESPONSE)
    # RugCheck API
    elif url.startswith(RUGCHECK_API_ENDPOINT_VAL):
        token_address_for_rugcheck = url.split("/")[-2]
        if "unsafe" in token_address_for_rugcheck: # Simple check for test
            mock_resp.json = AsyncMock(return_value=MOCK_RUGCHECK_RESPONSE_UNSAFE)
        else:
            mock_resp.json = AsyncMock(return_value=MOCK_RUGCHECK_RESPONSE_SAFE)
    else:
        mock_resp.status = 404
        mock_resp.json = AsyncMock(return_value={"error": "Mock URL not found"})

    # .text is often called by error handlers in aiohttp or by the SUT
    mock_resp.text = AsyncMock(return_value=json.dumps(await mock_resp.json()))
    return mock_resp


class TestTokenScanner(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.scanner = TokenScanner()
        self.scanner.rugcheck_jwt = None
        self.scanner.rugcheck_jwt_generation_attempted = False
        self.scanner.potential_tokens = []

    async def asyncSetUp(self):
        # Tests that don't mock session.get might need a real session for JWT part
        # Most tests will mock session.get directly.
        # We initialize it here to ensure self.scanner.session exists.
        # The actual JWT generation is tested separately.
        if not self.scanner.session or self.scanner.session.closed:
             # Temporarily disable JWT generation for most tests to avoid real calls
            with patch('trading_bot.scanner.RUGCHECK_AUTH_SOLANA_PRIVATE_KEY', None), \
                 patch('trading_bot.scanner.RUGCHECK_AUTH_WALLET_PUBLIC_KEY', None):
                await self.scanner.initialize()

    async def asyncTearDown(self):
        if self.scanner.session:
            await self.scanner.close()

    # --- Tests for _ensure_rugcheck_jwt (Keep these as they test a separate utility) ---
    # Make sure these tests create their own TokenScanner instances if they patch module-level config
    @patch('trading_bot.scanner.get_rugcheck_jwt', new_callable=AsyncMock)
    async def test_ensure_jwt_uses_static_if_present_and_no_dynamic_keys(self, mock_auth_get_jwt):
        with patch('trading_bot.config.STATIC_RUGCHECK_JWT', "static_jwt_token_from_config"), \
             patch('trading_bot.config.RUGCHECK_AUTH_SOLANA_PRIVATE_KEY', None), \
             patch('trading_bot.config.RUGCHECK_AUTH_WALLET_PUBLIC_KEY', None):
            # Use a fresh scanner instance for config isolation
            scanner_instance = TokenScanner()
            await scanner_instance.initialize() # This will call _ensure_rugcheck_jwt
            self.assertEqual(scanner_instance.rugcheck_jwt, "static_jwt_token_from_config")
            mock_auth_get_jwt.assert_not_called()
            await scanner_instance.close()

    # ... (Other _ensure_rugcheck_jwt tests can remain similar, ensuring fresh TokenScanner for config patches)


    # --- Tests for verify_token_safety_rugcheck (Uses the main mock_get_side_effect) ---
    @patch('aiohttp.ClientSession.get', new_callable=AsyncMock)
    async def test_rugcheck_safe_token(self, mock_session_get):
        mock_session_get.side_effect = mock_get_side_effect
        self.scanner.rugcheck_jwt = "test_jwt" # Assume JWT is set for this test
        result = await self.scanner.verify_token_safety_rugcheck(self.scanner.session, "any_safe_token_addr")
        self.assertTrue(result['is_safe'])
        self.assertEqual(result['score_normalised'], MOCK_RUGCHECK_RESPONSE_SAFE['scoreNormalised'])

    @patch('aiohttp.ClientSession.get', new_callable=AsyncMock)
    async def test_rugcheck_unsafe_token(self, mock_session_get):
        mock_session_get.side_effect = mock_get_side_effect
        result = await self.scanner.verify_token_safety_rugcheck(self.scanner.session, "any_unsafe_token_addr")
        self.assertFalse(result['is_safe'])
        self.assertEqual(result['score_normalised'], MOCK_RUGCHECK_RESPONSE_UNSAFE['scoreNormalised'])

    # --- Tests for analyze_token_metrics (Updated for new data structure and signature) ---
    @patch('trading_bot.scanner.TARGET_MARKET_CAP_TO_SCAN', MOCK_DETAILED_PAIR_DATA_VALID['fdv'] + 1000) # Make current fdv too low
    def test_analyze_metric_mcap_below_target(self):
        mock_data = MOCK_DETAILED_PAIR_DATA_VALID.copy()
        passed, reason = self.scanner.analyze_token_metrics(mock_data, MOCK_TOKEN_MINT_ADDR_1, MOCK_TOKEN_SYMBOL_1)
        self.assertFalse(passed)
        self.assertIn(f"MC ${mock_data['fdv']:,.0f} < Target ${TARGET_MARKET_CAP_TO_SCAN:,.0f}", reason)

    @patch('trading_bot.scanner.MAX_MARKET_CAP', MOCK_DETAILED_PAIR_DATA_VALID['fdv'] - 1000) # Make current fdv too high
    @patch('trading_bot.scanner.TARGET_MARKET_CAP_TO_SCAN', MOCK_DETAILED_PAIR_DATA_VALID['fdv'] // 2) # Ensure target mcap is met
    def test_analyze_metric_mcap_above_max(self):
        mock_data = MOCK_DETAILED_PAIR_DATA_VALID.copy()
        passed, reason = self.scanner.analyze_token_metrics(mock_data, MOCK_TOKEN_MINT_ADDR_1, MOCK_TOKEN_SYMBOL_1)
        self.assertFalse(passed)
        self.assertIn(f"MC ${mock_data['fdv']:,.0f} > Max ${MAX_MARKET_CAP:,.0f}", reason)

    @patch('trading_bot.scanner.MAX_TOKEN_AGE_HOURS', 0.5) # Require very new, MOCK_DETAILED_PAIR_DATA_VALID is 1h old
    def test_analyze_metric_token_too_old(self):
        mock_data = MOCK_DETAILED_PAIR_DATA_VALID.copy()
        # Ensure other metrics pass by setting permissive thresholds for them
        with patch('trading_bot.scanner.TARGET_MARKET_CAP_TO_SCAN', mock_data['fdv'] // 2), \
             patch('trading_bot.scanner.MAX_MARKET_CAP', mock_data['fdv'] * 2), \
             patch('trading_bot.scanner.MIN_LIQUIDITY', mock_data['liquidity']['usd'] // 2):
            passed, reason = self.scanner.analyze_token_metrics(mock_data, MOCK_TOKEN_MINT_ADDR_1, MOCK_TOKEN_SYMBOL_1)
        self.assertFalse(passed)
        self.assertIn(f"Token too old: 1.0h > {MAX_TOKEN_AGE_HOURS}h", reason)


    # --- Tests for scan_new_tokens (Refactored for two-phase API calls) ---
    @patch('trading_bot.scanner.FILTER_FOR_PUMPFUN_ONLY', False)
    @patch('aiohttp.ClientSession.get', new_callable=AsyncMock)
    # Mock verify_token_safety_rugcheck directly for some scan_new_tokens tests to isolate scanner logic
    @patch('trading_bot.scanner.TokenScanner.verify_token_safety_rugcheck', new_callable=AsyncMock)
    @patch('trading_bot.scanner.TokenScanner.get_social_sentiment_placeholder', new_callable=AsyncMock)
    async def test_scan_new_tokens_processes_valid_token_e2e(self, mock_social_sentiment, mock_rugcheck_method, mock_session_get):
        mock_session_get.side_effect = mock_get_side_effect # This will provide valid profile then valid pair data
        mock_rugcheck_method.return_value = MOCK_RUGCHECK_RESPONSE_SAFE
        mock_social_sentiment.return_value = {'sentiment_score': 0.5, 'sentiment': 'neutral'}

        # Set permissive metric configurations for this test
        with patch('trading_bot.scanner.TARGET_MARKET_CAP_TO_SCAN', MOCK_DETAILED_PAIR_DATA_VALID['fdv'] // 2), \
             patch('trading_bot.scanner.MAX_MARKET_CAP', MOCK_DETAILED_PAIR_DATA_VALID['fdv'] * 2), \
             patch('trading_bot.scanner.MAX_TOKEN_AGE_HOURS', 2), \
             patch('trading_bot.scanner.MIN_LIQUIDITY', MOCK_DETAILED_PAIR_DATA_VALID['liquidity']['usd'] // 2), \
             patch('trading_bot.scanner.MIN_TRANSACTIONS', MOCK_DETAILED_PAIR_DATA_VALID['txns']['h1']['buys'] // 2), \
             patch('trading_bot.scanner.MIN_BUY_SELL_RATIO', 0.1), \
             patch('trading_bot.scanner.VOLUME_SPIKE_THRESHOLD', 0.1):

            await self.scanner.scan_new_tokens()

        self.assertEqual(len(self.scanner.potential_tokens), 1)
        added_token = self.scanner.potential_tokens[0]
        self.assertEqual(added_token['address'], MOCK_TOKEN_MINT_ADDR_1)
        self.assertEqual(added_token['pair_address'], MOCK_PAIR_ADDR_1)
        self.assertIsNotNone(added_token['detailed_pair_data'])
        mock_rugcheck_method.assert_called_once_with(self.scanner.session, MOCK_TOKEN_MINT_ADDR_1)


    @patch('trading_bot.scanner.FILTER_FOR_PUMPFUN_ONLY', False)
    @patch('aiohttp.ClientSession.get', new_callable=AsyncMock)
    @patch('trading_bot.scanner.TokenScanner.verify_token_safety_rugcheck', new_callable=AsyncMock)
    async def test_scan_new_tokens_filters_failed_phase3_metrics(self, mock_rugcheck_method, mock_session_get):
        # Configure side_effect to serve a profile that leads to FAILS_METRICS pair data
        async def custom_side_effect(url, params=None, **kwargs):
            mock_resp = AsyncMock(spec=aiohttp.ClientResponse); mock_resp.status = 200
            mock_resp.__aenter__.return_value = mock_resp; mock_resp.__aexit__ = AsyncMock(return_value=None)
            if DEXSCREENER_TOKEN_PROFILES_API_VAL == url:
                profiles = [{"tokenAddress": MOCK_TOKEN_MINT_ADDR_2_FAILS_METRICS, "symbol": MOCK_TOKEN_SYMBOL_2_FAILS_METRICS}]
                mock_resp.json = AsyncMock(return_value=profiles)
            elif url.startswith(DEXSCREENER_SEARCH_API_VAL) and params.get('q') in [f"{MOCK_TOKEN_SYMBOL_2_FAILS_METRICS}/SOL", MOCK_TOKEN_MINT_ADDR_2_FAILS_METRICS]:
                mock_resp.json = AsyncMock(return_value=MOCK_PAIR_SEARCH_RESPONSE_FAILS_METRICS)
            else: mock_resp.json = AsyncMock(return_value=MOCK_EMPTY_PAIR_SEARCH_RESPONSE)
            mock_resp.text = AsyncMock(return_value=json.dumps(await mock_resp.json()))
            return mock_resp
        mock_session_get.side_effect = custom_side_effect

        # Default TARGET_MARKET_CAP_TO_SCAN is 30000. MOCK_DETAILED_PAIR_DATA_FAILS_METRICS has fdv: 100.
        await self.scanner.scan_new_tokens()
        self.assertEqual(len(self.scanner.potential_tokens), 0)
        mock_rugcheck_method.assert_not_called() # Should fail before rugcheck

    @patch('trading_bot.scanner.FILTER_FOR_PUMPFUN_ONLY', False)
    @patch('aiohttp.ClientSession.get', new_callable=AsyncMock)
    @patch('trading_bot.scanner.TokenScanner.verify_token_safety_rugcheck', new_callable=AsyncMock)
    async def test_scan_new_tokens_filters_unsafe_rugcheck(self, mock_rugcheck_method, mock_session_get):
        mock_session_get.side_effect = mock_get_side_effect # Serves valid profile & valid pair data by default
        mock_rugcheck_method.return_value = MOCK_RUGCHECK_RESPONSE_UNSAFE

        # Permissive metrics to ensure it reaches rugcheck
        with patch('trading_bot.scanner.TARGET_MARKET_CAP_TO_SCAN', 10000), \
             patch('trading_bot.scanner.MIN_LIQUIDITY', 1000):
            await self.scanner.scan_new_tokens()

        self.assertEqual(len(self.scanner.potential_tokens), 0)
        mock_rugcheck_method.assert_called_once_with(self.scanner.session, MOCK_TOKEN_MINT_ADDR_1)

    # --- Pump.fun filter tests updated ---
    @patch('trading_bot.scanner.FILTER_FOR_PUMPFUN_ONLY', True)
    @patch('trading_bot.scanner.PUMPFUN_ADDRESS_SUFFIX', "pump")
    @patch('aiohttp.ClientSession.get', new_callable=AsyncMock)
    @patch('trading_bot.scanner.TokenScanner.verify_token_safety_rugcheck', new_callable=AsyncMock)
    @patch('trading_bot.scanner.TokenScanner.get_social_sentiment_placeholder', new_callable=AsyncMock)
    async def test_scan_new_tokens_pumpfun_filter_active_match_e2e(self, mock_sentiment, mock_rugcheck_method, mock_session_get):
        # Side effect needs to handle profile and then search for "testtoken1pump"
        async def custom_pump_side_effect(url, params=None, **kwargs):
            mock_resp = AsyncMock(spec=aiohttp.ClientResponse); mock_resp.status = 200
            mock_resp.__aenter__.return_value = mock_resp; mock_resp.__aexit__ = AsyncMock(return_value=None)
            if DEXSCREENER_TOKEN_PROFILES_API_VAL == url:
                profiles = [{"tokenAddress": "testtoken1pump", "symbol": "PUMP1"}]
                mock_resp.json = AsyncMock(return_value=profiles)
            elif url.startswith(DEXSCREENER_SEARCH_API_VAL) and params.get('q') in ["PUMP1/SOL", "testtoken1pump"]:
                pair_data = MOCK_DETAILED_PAIR_DATA_VALID.copy()
                pair_data["baseToken"]["address"] = "testtoken1pump"; pair_data["baseToken"]["symbol"] = "PUMP1"
                mock_resp.json = AsyncMock(return_value={"pairs": [pair_data]})
            elif url.startswith(RUGCHECK_API_ENDPOINT_VAL): # if verify_token_safety_rugcheck is not mocked
                mock_resp.json = AsyncMock(return_value=MOCK_RUGCHECK_RESPONSE_SAFE)
            else: mock_resp.json = AsyncMock(return_value=MOCK_EMPTY_PAIR_SEARCH_RESPONSE)
            mock_resp.text = AsyncMock(return_value=json.dumps(await mock_resp.json()))
            return mock_resp

        mock_session_get.side_effect = custom_pump_side_effect
        mock_rugcheck_method.return_value = MOCK_RUGCHECK_RESPONSE_SAFE
        mock_sentiment.return_value = {'sentiment_score': 0.5, 'sentiment': 'neutral'}

        with patch('trading_bot.scanner.TARGET_MARKET_CAP_TO_SCAN', 10000): # Permissive
            await self.scanner.scan_new_tokens()

        self.assertEqual(len(self.scanner.potential_tokens), 1)
        self.assertEqual(self.scanner.potential_tokens[0]['address'], "testtoken1pump")
        mock_rugcheck_method.assert_called_once_with(self.scanner.session, "testtoken1pump")


    @patch('trading_bot.scanner.FILTER_FOR_PUMPFUN_ONLY', True)
    @patch('trading_bot.scanner.PUMPFUN_ADDRESS_SUFFIX', "pump")
    @patch('aiohttp.ClientSession.get', new_callable=AsyncMock)
    @patch('trading_bot.scanner.TokenScanner.verify_token_safety_rugcheck', new_callable=AsyncMock)
    async def test_scan_new_tokens_pumpfun_filter_active_no_match(self, mock_rugcheck_method, mock_session_get):
        async def custom_no_match_side_effect(url, params=None, **kwargs):
            mock_resp = AsyncMock(spec=aiohttp.ClientResponse); mock_resp.status = 200
            mock_resp.__aenter__.return_value = mock_resp; mock_resp.__aexit__ = AsyncMock(return_value=None)
            if DEXSCREENER_TOKEN_PROFILES_API_VAL == url:
                profiles = [{"tokenAddress": "testtoken2xyz", "symbol": "XYZ1"}] # No "pump" suffix
                mock_resp.json = AsyncMock(return_value=profiles)
            else: # Should not be called
                mock_resp.status = 500; mock_resp.json = AsyncMock(return_value={"error":"Should not call search/rugcheck"})
            mock_resp.text = AsyncMock(return_value=json.dumps(await mock_resp.json()))
            return mock_resp

        mock_session_get.side_effect = custom_no_match_side_effect
        await self.scanner.scan_new_tokens()
        self.assertEqual(len(self.scanner.potential_tokens), 0)

        # Verify that DEXSCREENER_SEARCH_API was not called
        called_search_api = any(
            DEXSCREENER_SEARCH_API_VAL in call[0][0]
            for call in mock_session_get.call_args_list
        )
        self.assertFalse(called_search_api, "Search API should not have been called due to pump.fun filter mismatch.")
        mock_rugcheck_method.assert_not_called()

if __name__ == '__main__':
    unittest.main()
