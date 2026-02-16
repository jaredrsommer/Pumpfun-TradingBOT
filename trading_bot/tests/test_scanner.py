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

# Updated to use score_normalised (lowercase_underscore)
MOCK_RUGCHECK_RESPONSE_SAFE = {"score_normalised": 5, "rugged": False, "risks": []}
MOCK_RUGCHECK_RESPONSE_UNSAFE = {"score_normalised": 50, "rugged": True, "risks": [{"name": "Rugpull", "level": "critical"}]}


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

    async def mock_rugcheck_response(self, status=200, score=None, score_normalised_val=None, risks=None, error_message=None): # Renamed score_normalised to score_normalised_val
        mock_resp = AsyncMock(spec=aiohttp.ClientResponse)
        mock_resp.status = status
        mock_resp.__aenter__.return_value = mock_resp
        mock_resp.__aexit__ = AsyncMock(return_value=None)

        if error_message: # Simulate API error before JSON parsing
            mock_resp.text = AsyncMock(return_value=error_message)
            mock_resp.json = AsyncMock(side_effect=aiohttp.ContentTypeError(None, None)) # Simulate failure if .json() is called
            return mock_resp

        response_json = {}
        if score is not None:
            response_json['score'] = score
        if score_normalised_val is not None: # Use the new param name
            response_json['score_normalised'] = score_normalised_val # Correct key
        response_json['risks'] = risks if risks is not None else []

        mock_resp.json = AsyncMock(return_value=response_json)
        mock_resp.text = AsyncMock(return_value=json.dumps(response_json))
        return mock_resp

    # --- Tests for verify_token_safety_rugcheck (Updated with specific score logic tests) ---

    @patch('trading_bot.scanner.RUGCHECK_SCORE_THRESHOLD', 10)
    @patch('aiohttp.ClientSession.get', new_callable=AsyncMock)
    async def test_rugcheck_score_normalised_preferred_and_valid(self, mock_session_get): # Renamed test
        mock_session_get.return_value = await self.mock_rugcheck_response(score_normalised_val=5, score=50)
        result = await self.scanner.verify_token_safety_rugcheck(self.scanner.session, "token_addr")
        self.assertTrue(result['is_safe'])
        self.assertEqual(result['score_normalised_api_raw'], 5) # Check the raw API value
        self.assertEqual(result['score_api_raw'], 50)
        self.assertEqual(result['score_value_used'], 5.0)
        self.assertNotIn("Score (5.00 from score_normalised) is above threshold (10).", result['reasons'])


    @patch('trading_bot.scanner.RUGCHECK_SCORE_THRESHOLD', 10)
    @patch('aiohttp.ClientSession.get', new_callable=AsyncMock)
    async def test_rugcheck_score_normalised_invalid_range_uses_valid_score(self, mock_session_get): # Renamed
        mock_session_get.return_value = await self.mock_rugcheck_response(score_normalised_val=160, score=8) # 160 is out of 0-150 range
        result = await self.scanner.verify_token_safety_rugcheck(self.scanner.session, "token_addr")
        self.assertTrue(result['is_safe']) # score=8 is used
        self.assertEqual(result['score_normalised_api_raw'], 160)
        self.assertEqual(result['score_api_raw'], 8)
        self.assertEqual(result['score_value_used'], 8.0)
        self.assertNotIn("Score (8.00 from score) is above threshold (10).", result['reasons'])

    @patch('trading_bot.scanner.RUGCHECK_SCORE_THRESHOLD', 10)
    @patch('aiohttp.ClientSession.get', new_callable=AsyncMock)
    async def test_rugcheck_score_normalised_non_numeric_uses_valid_score(self, mock_session_get): # Renamed
        mock_session_get.return_value = await self.mock_rugcheck_response(score_normalised_val="error", score=8)
        result = await self.scanner.verify_token_safety_rugcheck(self.scanner.session, "token_addr")
        self.assertTrue(result['is_safe']) # score=8 is used
        self.assertEqual(result['score_normalised_api_raw'], "error")
        self.assertEqual(result['score_api_raw'], 8)
        self.assertEqual(result['score_value_used'], 8.0)


    @patch('trading_bot.scanner.RUGCHECK_SCORE_THRESHOLD', 10)
    @patch('aiohttp.ClientSession.get', new_callable=AsyncMock)
    async def test_rugcheck_score_normalised_valid_score_absent(self, mock_session_get): # Renamed
        mock_session_get.return_value = await self.mock_rugcheck_response(score_normalised_val=5, score=None)
        result = await self.scanner.verify_token_safety_rugcheck(self.scanner.session, "token_addr")
        self.assertTrue(result['is_safe'])
        self.assertEqual(result['score_normalised_api_raw'], 5)
        self.assertIsNone(result['score_api_raw'])
        self.assertEqual(result['score_value_used'], 5.0)


    @patch('trading_bot.scanner.RUGCHECK_SCORE_THRESHOLD', 10)
    @patch('aiohttp.ClientSession.get', new_callable=AsyncMock)
    async def test_rugcheck_score_normalised_absent_score_valid(self, mock_session_get): # Renamed
        mock_session_get.return_value = await self.mock_rugcheck_response(score_normalised_val=None, score=5)
        result = await self.scanner.verify_token_safety_rugcheck(self.scanner.session, "token_addr")
        self.assertTrue(result['is_safe'])
        self.assertIsNone(result['score_normalised_api_raw'])
        self.assertEqual(result['score_api_raw'], 5)
        self.assertEqual(result['score_value_used'], 5.0)


    @patch('aiohttp.ClientSession.get', new_callable=AsyncMock)
    async def test_rugcheck_both_scores_invalid_range(self, mock_session_get):
        mock_session_get.return_value = await self.mock_rugcheck_response(score_normalised_val=170, score=-10)
        result = await self.scanner.verify_token_safety_rugcheck(self.scanner.session, "token_addr")
        self.assertFalse(result['is_safe'])
        self.assertIn("No valid score (normalised or raw) available from RugCheck after validation.", result['reasons'])
        self.assertIsNone(result['score_value_used'])


    @patch('aiohttp.ClientSession.get', new_callable=AsyncMock)
    async def test_rugcheck_both_scores_non_numeric(self, mock_session_get):
        mock_session_get.return_value = await self.mock_rugcheck_response(score_normalised_val="error1", score="error2")
        result = await self.scanner.verify_token_safety_rugcheck(self.scanner.session, "token_addr")
        self.assertFalse(result['is_safe'])
        self.assertIn("No valid score (normalised or raw) available from RugCheck after validation.", result['reasons'])
        self.assertIsNone(result['score_value_used'])

    @patch('aiohttp.ClientSession.get', new_callable=AsyncMock)
    async def test_rugcheck_both_scores_none(self, mock_session_get):
        mock_session_get.return_value = await self.mock_rugcheck_response(score_normalised_val=None, score=None)
        result = await self.scanner.verify_token_safety_rugcheck(self.scanner.session, "token_addr")
        self.assertFalse(result['is_safe'])
        self.assertIn("No valid score (normalised or raw) available from RugCheck after validation.", result['reasons'])
        self.assertIsNone(result['score_value_used'])

    @patch('trading_bot.scanner.RUGCHECK_SCORE_THRESHOLD', 10)
    @patch('aiohttp.ClientSession.get', new_callable=AsyncMock)
    async def test_rugcheck_score_at_threshold(self, mock_session_get):
        mock_session_get.return_value = await self.mock_rugcheck_response(score_normalised_val=10)
        result = await self.scanner.verify_token_safety_rugcheck(self.scanner.session, "token_addr")
        self.assertTrue(result['is_safe'])
        self.assertEqual(result['score_value_used'], 10.0)

    @patch('trading_bot.scanner.RUGCHECK_SCORE_THRESHOLD', 10)
    @patch('aiohttp.ClientSession.get', new_callable=AsyncMock)
    async def test_rugcheck_score_just_above_threshold(self, mock_session_get):
        mock_session_get.return_value = await self.mock_rugcheck_response(score_normalised_val=10.1)
        result = await self.scanner.verify_token_safety_rugcheck(self.scanner.session, "token_addr")
        self.assertFalse(result['is_safe'])
        self.assertEqual(result['score_value_used'], 10.1)
        self.assertIn("Score (10.10 from score_normalised) is above threshold (10).", result['reasons'])


    @patch('trading_bot.scanner.RUGCHECK_SCORE_THRESHOLD', 10)
    @patch('aiohttp.ClientSession.get', new_callable=AsyncMock)
    async def test_rugcheck_score_at_valid_max_above_threshold(self, mock_session_get):
        mock_session_get.return_value = await self.mock_rugcheck_response(score_normalised_val=150.0) # VALID_SCORE_MAX is 150
        result = await self.scanner.verify_token_safety_rugcheck(self.scanner.session, "token_addr")
        self.assertFalse(result['is_safe'])
        self.assertEqual(result['score_value_used'], 150.0)
        self.assertIn("Score (150.00 from score_normalised) is above threshold (10).", result['reasons'])

    @patch('trading_bot.scanner.RUGCHECK_SCORE_THRESHOLD', 20)
    @patch('aiohttp.ClientSession.get', new_callable=AsyncMock)
    async def test_rugcheck_problematic_reported_value_safe_fallback(self, mock_session_get): # Renamed
        mock_session_get.return_value = await self.mock_rugcheck_response(score_normalised_val="501", score="16.4")
        result = await self.scanner.verify_token_safety_rugcheck(self.scanner.session, "token_addr")
        self.assertTrue(result['is_safe'])
        self.assertEqual(result['score_normalised_api_raw'], "501")
        self.assertEqual(result['score_api_raw'], "16.4")
        self.assertEqual(result['score_value_used'], 16.4)
        self.assertNotIn(f"Score (16.40 from score) is above threshold (20).", result['reasons'])

    @patch('trading_bot.scanner.RUGCHECK_SCORE_THRESHOLD', 5)
    @patch('aiohttp.ClientSession.get', new_callable=AsyncMock)
    async def test_rugcheck_problematic_reported_value_unsafe_fallback(self, mock_session_get): # Renamed
        mock_session_get.return_value = await self.mock_rugcheck_response(score_normalised_val="501", score="16.4")
        result = await self.scanner.verify_token_safety_rugcheck(self.scanner.session, "token_addr")
        self.assertFalse(result['is_safe'])
        self.assertEqual(result['score_value_used'], 16.4)
        self.assertIn(f"Score (16.40 from score) is above threshold (5).", result['reasons'])

    @patch('trading_bot.scanner.RUGCHECK_SCORE_THRESHOLD', 10)
    @patch('aiohttp.ClientSession.get', new_callable=AsyncMock)
    async def test_rugcheck_critical_risk_overrides_good_score(self, mock_session_get):
        critical_risks = [{"name": "Honeypot", "level": "critical", "description": "It's a trap!"}]
        mock_session_get.return_value = await self.mock_rugcheck_response(score_normalised_val=5, risks=critical_risks) # Good score
        with patch('trading_bot.scanner.RUGCHECK_CRITICAL_RISK_NAMES', ["Honeypot"]):
            result = await self.scanner.verify_token_safety_rugcheck(self.scanner.session, "token_addr")
        self.assertFalse(result['is_safe'])
        self.assertTrue(any("Critical risk: Honeypot" in reason for reason in result['reasons']))

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

    # --- Tests for get_scanner_metrics ---
    def _create_mock_potential_token(self, mint_addr, symbol_profile, symbol_pair, pair_addr, price_usd, discovered_mins_ago):
        discovery_time = datetime.now() - timedelta(minutes=discovered_mins_ago)
        return {
            'address': mint_addr,
            'symbol': symbol_profile, # This is the one from Phase 1 profile
            'pair_address': pair_addr,
            'phase1_discovered_at': discovery_time,
            'timestamp': discovery_time,
            'detailed_pair_data': {
                'baseToken': {'symbol': symbol_pair}, # This is symbol from pair data
                'pairAddress': pair_addr,
                'priceUsd': str(price_usd) # Store as string, as API might
            },
            # Add other fields if get_scanner_metrics starts using them
        }

    def test_get_scanner_metrics_initial_state(self):
        initial_time = self.scanner.last_scan_reset_time # Capture before calling
        metrics = self.scanner.get_scanner_metrics()
        self.assertEqual(metrics['unique_tokens_scanned_today'], 0)
        self.assertEqual(metrics['total_unique_tokens_scanned_all_time'], 0)
        self.assertEqual(metrics['potential_tokens_count'], 0)
        self.assertEqual(metrics['potential_tokens_recent'], [])
        self.assertEqual(metrics['rugcheck_jwt_status'], "Not Loaded") # Assuming default setUp state
        self.assertEqual(metrics['last_daily_reset_at'], initial_time.isoformat())

    def test_get_scanner_metrics_after_scanning_some_tokens(self):
        # Manually populate scanner state
        self.scanner.unique_tokens_scanned_today = {'addr1', 'addr2'}
        self.scanner.total_unique_tokens_ever = {'addr1', 'addr2', 'addr3'}

        token1_time = datetime.now() - timedelta(minutes=10)
        token2_time = datetime.now() - timedelta(minutes=5)

        self.scanner.potential_tokens = [
            self._create_mock_potential_token("addr1", "SYM1P", "SYM1", "pair1", 1.0, 10),
            self._create_mock_potential_token("addr2", "SYM2P", "SYM2", "pair2", 2.55555555, 5)
        ]
        self.scanner.potential_tokens[0]['phase1_discovered_at'] = token1_time
        self.scanner.potential_tokens[1]['phase1_discovered_at'] = token2_time


        metrics = self.scanner.get_scanner_metrics()

        self.assertEqual(metrics['unique_tokens_scanned_today'], 2)
        self.assertEqual(metrics['total_unique_tokens_scanned_all_time'], 3)
        self.assertEqual(metrics['potential_tokens_count'], 2)
        self.assertEqual(len(metrics['potential_tokens_recent']), 2)

        # Check recent token details (order is latest first if sliced with [-N:])
        # Actually, the current implementation takes last N, so order is preserved.
        recent1 = metrics['potential_tokens_recent'][0]
        self.assertEqual(recent1['symbol'], 'SYM1') # From pair data
        self.assertTrue('pair1' in recent1['pair_address_short'])
        self.assertEqual(recent1['price_usd'], "1.000000")
        self.assertEqual(recent1['discovered_at'], token1_time.isoformat())

        recent2 = metrics['potential_tokens_recent'][1]
        self.assertEqual(recent2['symbol'], 'SYM2')
        self.assertTrue('pair2' in recent2['pair_address_short'])
        self.assertEqual(recent2['price_usd'], "2.555556") # Check rounding/formatting
        self.assertEqual(recent2['discovered_at'], token2_time.isoformat())

    @patch('trading_bot.scanner.datetime') # Mock datetime module within scanner.py
    @patch('aiohttp.ClientSession.get', new_callable=AsyncMock) # Mock API calls during scan
    async def test_get_scanner_metrics_daily_reset(self, mock_session_get, mock_datetime):
        # Setup initial time and add some scanned tokens
        initial_real_time = datetime.now()
        mock_datetime.now.return_value = initial_real_time
        self.scanner.last_scan_reset_time = initial_real_time
        self.scanner.unique_tokens_scanned_today = {'addr1', 'addr2'}
        self.scanner.total_unique_tokens_ever = {'addr1', 'addr2', 'addr3'}

        metrics_before_reset = self.scanner.get_scanner_metrics()
        self.assertEqual(metrics_before_reset['unique_tokens_scanned_today'], 2)
        self.assertEqual(metrics_before_reset['last_daily_reset_at'], initial_real_time.isoformat())

        # Simulate time passing (25 hours later)
        time_after_25_hours = initial_real_time + timedelta(hours=25)
        mock_datetime.now.return_value = time_after_25_hours

        # Mock the API call within scan_new_tokens to return empty list,
        # so it runs quickly and triggers reset logic without processing tokens.
        mock_session_get.side_effect = mock_get_side_effect # Use general mock, ensure it can return empty profiles

        # For this specific test, make sure token profiles API returns empty
        async def empty_profile_side_effect(url, params=None, **kwargs):
            mock_resp = AsyncMock(spec=aiohttp.ClientResponse); mock_resp.status = 200
            mock_resp.__aenter__.return_value = mock_resp; mock_resp.__aexit__ = AsyncMock(return_value=None)
            if DEXSCREENER_TOKEN_PROFILES_API_VAL == url:
                mock_resp.json = AsyncMock(return_value=[]) # Empty list of profiles
            else: # Fallback to general mock for other potential calls (e.g. rugcheck if any token slipped through)
                return await mock_get_side_effect(url, params, **kwargs)
            mock_resp.text = AsyncMock(return_value=json.dumps(await mock_resp.json()))
            return mock_resp
        mock_session_get.side_effect = empty_profile_side_effect

        await self.scanner.scan_new_tokens() # This should trigger the reset

        metrics_after_reset = self.scanner.get_scanner_metrics()
        self.assertEqual(metrics_after_reset['unique_tokens_scanned_today'], 0)
        self.assertEqual(metrics_after_reset['total_unique_tokens_scanned_all_time'], 3) # Should not change
        self.assertEqual(metrics_after_reset['last_daily_reset_at'], time_after_25_hours.isoformat())


    def test_get_scanner_metrics_potential_tokens_recent_limit_and_formatting(self):
        now = datetime.now()
        self.scanner.potential_tokens = [
            self._create_mock_potential_token("addr1", "S1P", "S1", "pair1", 0.000012345, 60), # Oldest
            self._create_mock_potential_token("addr2", "S2P", "S2", "pair2", 2.0, 30),
            self._create_mock_potential_token("addr3", "S3P", "S3", "pair3", 30.12, 10),
            self._create_mock_potential_token("addr4", "S4P", "S4", "pair4", 123.0, 1)    # Newest
        ]
        # Update timestamps precisely
        for i, mins_ago in enumerate([60,30,10,1]):
            self.scanner.potential_tokens[i]['phase1_discovered_at'] = now - timedelta(minutes=mins_ago)


        metrics = self.scanner.get_scanner_metrics()
        self.assertEqual(len(metrics['potential_tokens_recent']), 3) # Max 3
        # Tokens should be the latest 3: addr4, addr3, addr2
        self.assertEqual(metrics['potential_tokens_recent'][0]['symbol'], 'S2') # -N returns last N items in original order
        self.assertEqual(metrics['potential_tokens_recent'][1]['symbol'], 'S3')
        self.assertEqual(metrics['potential_tokens_recent'][2]['symbol'], 'S4')

        # Test formatting for one of them
        token_s4_details = metrics['potential_tokens_recent'][2]
        self.assertEqual(token_s4_details['price_usd'], "123.000000")
        self.assertEqual(token_s4_details['discovered_at'], (now - timedelta(minutes=1)).isoformat())
        self.assertTrue("pair4" in token_s4_details['pair_address_short'])

        # Test with missing detailed_pair_data and priceUsd
        missing_data_token_time = now - timedelta(minutes=5)
        self.scanner.potential_tokens.append({
            'address': 'addr5_missing', 'symbol': 'S5P_NODETAIL', 'pair_address': 'pair5miss',
            'phase1_discovered_at': missing_data_token_time, 'timestamp': missing_data_token_time,
            'detailed_pair_data': {} # Missing priceUsd and baseToken.symbol
        })
        metrics_with_missing = self.scanner.get_scanner_metrics()
        self.assertEqual(len(metrics_with_missing['potential_tokens_recent']), 3)

        # The newest one is addr5_missing
        token_s5_details = metrics_with_missing['potential_tokens_recent'][2]
        self.assertEqual(token_s5_details['symbol'], 'S5P_NODETAIL') # Fallback to profile symbol
        self.assertEqual(token_s5_details['price_usd'], "N/A")
        self.assertTrue("pair5miss" in token_s5_details['pair_address_short'])
        self.assertEqual(token_s5_details['discovered_at'], missing_data_token_time.isoformat())

        # Test with priceUsd being non-numeric string
        non_numeric_price_token_time = now - timedelta(minutes=2)
        self.scanner.potential_tokens = [ # Reset to control order
             self._create_mock_potential_token("addr4", "S4P", "S4", "pair4", 123.0, 10),
             {
                'address': 'addr6_badprice', 'symbol': 'S6P', 'pair_address': 'pair6bad',
                'phase1_discovered_at': non_numeric_price_token_time, 'timestamp': non_numeric_price_token_time,
                'detailed_pair_data': {'baseToken': {'symbol': 'S6'}, 'priceUsd': "NotANumber"}
            },
            self._create_mock_potential_token("addr7", "S7P", "S7", "pair7", 10.0, 1)
        ]
        for i, mins_ago in enumerate([10,2,1]): # Update timestamps for these three
             self.scanner.potential_tokens[i]['phase1_discovered_at'] = now - timedelta(minutes=mins_ago)


        metrics_bad_price = self.scanner.get_scanner_metrics()
        token_s6_details = metrics_bad_price['potential_tokens_recent'][1] # Middle element
        self.assertEqual(token_s6_details['symbol'], 'S6')
        self.assertEqual(token_s6_details['price_usd'], "NotANumber") # Should keep original string


if __name__ == '__main__':
    unittest.main()
