import asyncio
import logging
from datetime import datetime, timedelta
import aiohttp
import pandas as pd
from typing import Optional # For type hinting

# Import necessary config variables explicitly for clarity and correctness
from .config import (
    DEXSCREENER_TOKEN_PROFILES_API, DEXSCREENER_SEARCH_API, # Updated API vars
    TARGET_MARKET_CAP_TO_SCAN, MAX_MARKET_CAP, MAX_TOKEN_AGE_HOURS,
    MIN_LIQUIDITY, MIN_TRANSACTIONS, MIN_BUY_SELL_RATIO, VOLUME_SPIKE_THRESHOLD,
    MIN_HOLDER_COUNT,
    RUGCHECK_API_ENDPOINT,
    STATIC_RUGCHECK_JWT,
    RUGCHECK_AUTH_SOLANA_PRIVATE_KEY,
    RUGCHECK_AUTH_WALLET_PUBLIC_KEY,
    RUGCHECK_SCORE_THRESHOLD, RUGCHECK_CRITICAL_RISK_NAMES,
    FILTER_FOR_PUMPFUN_ONLY, PUMPFUN_ADDRESS_SUFFIX # Added Pump.fun filters
)
from .auth_utils import get_rugcheck_jwt


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Helper function for shortening addresses in logs
def _shorten_address(address: str, chars: int = 4) -> str:
    if not address or len(address) <= chars * 2:
        return address
    return f"{address[:chars]}...{address[-chars:]}"

class TokenScanner:
    def __init__(self):
        self.session: Optional[aiohttp.ClientSession] = None
        self.potential_tokens = []
        self.scan_count = 0
        self.rugcheck_jwt: Optional[str] = STATIC_RUGCHECK_JWT
        self.rugcheck_jwt_generation_attempted: bool = False
        # Metrics for unique tokens scanned
        self.unique_tokens_scanned_today = set()
        self.last_scan_reset_time = datetime.now()
        self.total_unique_tokens_ever = set()

    async def _ensure_rugcheck_jwt(self) -> None:
        """
        Ensures a RugCheck.xyz JWT is available, attempting to generate one if needed.
        Uses a static JWT from config (STATIC_RUGCHECK_API_KEY_OR_JWT) if available and no dynamic keys are set.
        If dynamic keys (RUGCHECK_AUTH_SOLANA_PRIVATE_KEY & RUGCHECK_AUTH_WALLET_PUBLIC_KEY) are set,
        it will attempt to generate a new JWT, potentially overwriting the static one.
        Sets self.rugcheck_jwt.
        """
        # If a static JWT is already loaded and we are not configured for dynamic generation,
        # or if dynamic generation was already attempted, respect the current state.
        if self.rugcheck_jwt and not (RUGCHECK_AUTH_SOLANA_PRIVATE_KEY and RUGCHECK_AUTH_WALLET_PUBLIC_KEY):
            logger.info(f"Using static RugCheck JWT: {'Yes' if self.rugcheck_jwt else 'No'}")
            return

        if self.rugcheck_jwt_generation_attempted:
            logger.debug("Skipping RugCheck JWT dynamic generation attempt as it was already tried.")
            return

        self.rugcheck_jwt_generation_attempted = True # Mark that we are trying dynamic generation now

        if RUGCHECK_AUTH_SOLANA_PRIVATE_KEY and RUGCHECK_AUTH_WALLET_PUBLIC_KEY:
            logger.info("Attempting to dynamically generate RugCheck.xyz JWT using configured private/public key pair...")

            session_to_use = self.session
            temp_session_created = False
            if not session_to_use or session_to_use.closed:
                logger.warning("TokenScanner session not available or closed for JWT generation. Creating temporary session.")
                session_to_use = aiohttp.ClientSession()
                temp_session_created = True

            try:
                generated_jwt = await get_rugcheck_jwt(
                    session_to_use,
                    RUGCHECK_AUTH_SOLANA_PRIVATE_KEY,
                    RUGCHECK_AUTH_WALLET_PUBLIC_KEY
                    # auth_url can be added here if it needs to be configurable too
                )
                if generated_jwt:
                    self.rugcheck_jwt = generated_jwt # This overwrites any static key/JWT
                    logger.info("Successfully generated and set new RugCheck.xyz JWT.")
                else:
                    logger.warning("Failed to dynamically generate RugCheck.xyz JWT. "
                                   f"Will rely on static JWT if previously set ('{STATIC_RUGCHECK_JWT is not None}'), "
                                   "or proceed unauthenticated for RugCheck.")
                    # If dynamic fails, and a static one was there, self.rugcheck_jwt retains the static one (initial value from __init__).
                    # If no static one was there, self.rugcheck_jwt remains None (or whatever it was if dynamic gen failed).
                    if not STATIC_RUGCHECK_JWT: # If no static JWT was configured to fall back on
                        self.rugcheck_jwt = None # Ensure it's None if dynamic fails and no static was there
            finally:
                if temp_session_created:
                    await session_to_use.close()
                    logger.debug("Temporary session for JWT generation closed.")
        elif self.rugcheck_jwt: # No dynamic keys, but static JWT was present
             logger.info(f"Using static RugCheck JWT. Dynamic generation not configured (missing private/public keys).")
        else: # No static JWT and no dynamic keys
            logger.info("No static RugCheck JWT provided and no private/public keys configured for dynamic JWT generation. RugCheck requests will be unauthenticated.")


    async def initialize(self):
        if not self.session or self.session.closed: # Ensure session is only created if needed
            self.session = aiohttp.ClientSession()
            logger.info("Token scanner aiohttp session initialized.")
        else:
            logger.info("Token scanner aiohttp session already initialized.")
        await self._ensure_rugcheck_jwt() # Attempt to get JWT after session is ready

    async def close(self):
        if self.session:
            await self.session.close()

    def analyze_token_metrics(self, detailed_pair_data, token_address_from_profile=None, token_symbol_from_profile=None):
        """Phase 3: Analyze token metrics based on detailed_pair_data."""
        actual_symbol = detailed_pair_data.get('baseToken', {}).get('symbol') or token_symbol_from_profile or "UnknownSymbol"
        pair_address_short = _shorten_address(detailed_pair_data.get('pairAddress', 'UnknownPairAddr'))

        log_prefix_phase3 = f"📊 Token ${actual_symbol} (Pair: {pair_address_short}) (Phase 3):"
        logger.info(f"{log_prefix_phase3} Starting metric analysis.")

        try:
            if not detailed_pair_data:
                logger.warning(f"{log_prefix_phase3} ❌ No detailed pair data provided. Skipping. ⏭️")
                return False, "No detailed pair data provided"

            # Market Cap (FDV) Check
            fdv_data = detailed_pair_data.get('fdv')
            if fdv_data is None:
                logger.info(f"{log_prefix_phase3}   ❌ Market Cap Failed: Missing FDV data. Skipping. ⏭️")
                return False, "Missing FDV (market cap)"
            try: market_cap = float(fdv_data)
            except (ValueError, TypeError):
                logger.info(f"{log_prefix_phase3}   ❌ Market Cap Failed: Invalid FDV data type ({fdv_data}). Skipping. ⏭️")
                return False, f"Invalid FDV data type: {fdv_data}"
            if market_cap < TARGET_MARKET_CAP_TO_SCAN:
                logger.info(f"{log_prefix_phase3}   ❌ Market Cap Failed: ${market_cap:,.0f} < Target ${TARGET_MARKET_CAP_TO_SCAN:,.0f}. Skipping. ⏭️")
                return False, f"MC ${market_cap:,.0f} < Target ${TARGET_MARKET_CAP_TO_SCAN:,.0f}"
            if market_cap > MAX_MARKET_CAP:
                logger.info(f"{log_prefix_phase3}   ❌ Market Cap Failed: ${market_cap:,.0f} > Max ${MAX_MARKET_CAP:,.0f}. Skipping. ⏭️")
                return False, f"MC ${market_cap:,.0f} > Max ${MAX_MARKET_CAP:,.0f}"
            logger.info(f"{log_prefix_phase3}   ✅ Market Cap Passed: Target ${TARGET_MARKET_CAP_TO_SCAN:,.0f} <= Actual ${market_cap:,.0f} <= Max ${MAX_MARKET_CAP:,.0f}")

            # Token Age (Pair Creation Time) Check
            created_timestamp = detailed_pair_data.get('pairCreatedAt')
            if created_timestamp is None:
                logger.info(f"{log_prefix_phase3}   ❌ Age Failed: Missing pairCreatedAt. Skipping. ⏭️")
                return False, "Missing pairCreatedAt"
            try: created_at = datetime.fromtimestamp(created_timestamp / 1000)
            except (TypeError, ValueError):
                logger.info(f"{log_prefix_phase3}   ❌ Age Failed: Invalid pairCreatedAt timestamp ({created_timestamp}). Skipping. ⏭️")
                return False, f"Invalid pairCreatedAt timestamp: {created_timestamp}"
            age_hours = (datetime.now() - created_at).total_seconds() / 3600
            if age_hours > MAX_TOKEN_AGE_HOURS:
                logger.info(f"{log_prefix_phase3}   ❌ Age Failed: {age_hours:.1f}h > {MAX_TOKEN_AGE_HOURS}h. Skipping. ⏭️")
                return False, f"Token too old: {age_hours:.1f}h > {MAX_TOKEN_AGE_HOURS}h"
            logger.info(f"{log_prefix_phase3}   ✅ Age Passed: {age_hours:.1f}h <= {MAX_TOKEN_AGE_HOURS}h")

            # Liquidity Check
            liquidity_data = detailed_pair_data.get('liquidity')
            if not liquidity_data or liquidity_data.get('usd') is None:
                logger.info(f"{log_prefix_phase3}   ❌ Liquidity Failed: Missing liquidity USD. Skipping. ⏭️")
                return False, "Missing liquidity USD"
            try: liquidity_usd = float(liquidity_data['usd'])
            except ValueError:
                logger.info(f"{log_prefix_phase3}   ❌ Liquidity Failed: Invalid liquidity USD type ({liquidity_data['usd']}). Skipping. ⏭️")
                return False, f"Invalid liquidity USD type: {liquidity_data['usd']}"
            if liquidity_usd < MIN_LIQUIDITY:
                logger.info(f"{log_prefix_phase3}   ❌ Liquidity Failed: ${liquidity_usd:,.0f} < Min ${MIN_LIQUIDITY:,.0f}. Skipping. ⏭️")
                return False, f"Liquidity ${liquidity_usd:,.0f} < Min ${MIN_LIQUIDITY:,.0f}"
            logger.info(f"{log_prefix_phase3}   ✅ Liquidity Passed: ${liquidity_usd:,.0f} >= ${MIN_LIQUIDITY:,.0f}")

            # Transactions Check
            txns_h1_data = detailed_pair_data.get('txns', {}).get('h1', {})
            buys = txns_h1_data.get('buys', 0); sells = txns_h1_data.get('sells', 0)
            if (buys + sells) < MIN_TRANSACTIONS:
                logger.info(f"{log_prefix_phase3}   ❌ Transactions Failed: (1h) {buys+sells} < Min {MIN_TRANSACTIONS}. Skipping. ⏭️")
                return False, f"Txns (1h) {buys+sells} < Min {MIN_TRANSACTIONS}"
            logger.info(f"{log_prefix_phase3}   ✅ Transaction Count Passed: {buys+sells} >= {MIN_TRANSACTIONS}")

            current_buy_sell_ratio = self._calculate_buy_sell_ratio(detailed_pair_data)
            if sells > 0 and current_buy_sell_ratio < MIN_BUY_SELL_RATIO:
                logger.info(f"{log_prefix_phase3}   ❌ Buy/Sell Ratio Failed: {current_buy_sell_ratio:.2f} < Min {MIN_BUY_SELL_RATIO}. Skipping. ⏭️")
                return False, f"Buy/Sell ratio {current_buy_sell_ratio:.2f} < Min {MIN_BUY_SELL_RATIO}"
            logger.info(f"{log_prefix_phase3}   ✅ Buy/Sell Ratio Passed: {current_buy_sell_ratio:.2f} >= {MIN_BUY_SELL_RATIO}")

            # Volume Spike Check
            volume_data = detailed_pair_data.get('volume', {})
            try: volume_1h = float(volume_data.get('h1', 0.0)); volume_24h = float(volume_data.get('h24', 0.0))
            except ValueError:
                logger.info(f"{log_prefix_phase3}   ❌ Volume Spike Failed: Invalid volume data. Skipping. ⏭️")
                return False, f"Invalid volume data type. H1: {volume_data.get('h1')}, H24: {volume_data.get('h24')}"

            if VOLUME_SPIKE_THRESHOLD > 0:
                if volume_24h > 0 and volume_1h > 0:
                    hourly_equiv_from_24h = volume_24h / 24
                    if hourly_equiv_from_24h == 0:
                        if volume_1h > 0: logger.info(f"{log_prefix_phase3}   ✅ Volume Spike Passed (1h vol > 0, 24h avg vol = 0)")
                        else:
                            logger.info(f"{log_prefix_phase3}   ❌ Volume Spike Failed: No 1h volume and 24h avg is 0. Skipping. ⏭️")
                            return False, "No volume spike: 1h vol is 0 and 24h avg vol is 0."
                    elif (volume_1h / hourly_equiv_from_24h) < VOLUME_SPIKE_THRESHOLD:
                        logger.info(f"{log_prefix_phase3}   ❌ Volume Spike Failed: (1h/avg_1h_from_24h) {(volume_1h / hourly_equiv_from_24h):.1f}x < {VOLUME_SPIKE_THRESHOLD}x. Skipping. ⏭️")
                        return False, f"No volume spike: (1h/avg_1h_from_24h) {(volume_1h / hourly_equiv_from_24h):.1f}x < {VOLUME_SPIKE_THRESHOLD}x"
                    else: logger.info(f"{log_prefix_phase3}   ✅ Volume Spike Passed: {(volume_1h / hourly_equiv_from_24h):.1f}x >= {VOLUME_SPIKE_THRESHOLD}x")
                elif volume_1h > 0 and volume_24h == 0: logger.info(f"{log_prefix_phase3}   ✅ Volume Spike Passed (1h vol > 0, 24h vol = 0)")
                else:
                    logger.info(f"{log_prefix_phase3}   ❌ Volume Spike Failed: 1h vol is {volume_1h}, 24h vol is {volume_24h}. Threshold: {VOLUME_SPIKE_THRESHOLD}x. Skipping. ⏭️")
                    return False, f"No volume spike: 1h vol is {volume_1h}, 24h vol is {volume_24h}. Threshold: {VOLUME_SPIKE_THRESHOLD}x"
            else: logger.info(f"{log_prefix_phase3}   ℹ️ Volume spike check skipped (threshold is 0).")

            # Price Change Check (1h)
            price_change_1h = float(detailed_pair_data.get('priceChange', {}).get('h1', 0.0))
            if price_change_1h < -25: # Example threshold
                logger.info(f"{log_prefix_phase3}   ❌ Price Change Failed: (1h) {price_change_1h}% < -25%. Skipping. ⏭️")
                return False, f"Price drop (1h): {price_change_1h}% < -25%"
            logger.info(f"{log_prefix_phase3}   ✅ Price Change (1h) Passed: {price_change_1h}% >= -25%")

            logger.info(f"{log_prefix_phase3}   🎉 All metric checks passed.")
            return True, "Token passed all Phase 3 checks"
        except Exception as e:
            logger.error(f"{log_prefix_phase3} Error in analyze_token_metrics (Phase 3): {e}", exc_info=True)
            return False, str(e)

    async def scan_new_tokens(self):
        """Phase 1 & 2 & 3: Discover, Fetch Details, and Analyze Tokens."""
        try:
            # Daily reset logic for unique scanned tokens
            if datetime.now() - self.last_scan_reset_time > timedelta(hours=24):
                logger.info(f"🔄 Resetting daily unique token scan count. Previous count: {len(self.unique_tokens_scanned_today)}")
                self.unique_tokens_scanned_today = set()
                self.last_scan_reset_time = datetime.now()

            # Use DEXSCREENER_TOKEN_PROFILES_API for Phase 1
            params = {"chainId": "solana"}
            logger.info(f"🔄 Starting scan for new token profiles using {DEXSCREENER_TOKEN_PROFILES_API} with params {params}")
            async with self.session.get(DEXSCREENER_TOKEN_PROFILES_API, params=params) as response:
                response.raise_for_status()
                token_profiles = await response.json()
                self.scan_count += 1
                logger.info(f"🔄 Scan Iteration {self.scan_count}: Discovered {len(token_profiles)} token profiles.")

                for token_profile in token_profiles:
                    base_token_address = token_profile.get('tokenAddress')
                    if not base_token_address:
                        logger.debug("Skipping profile due to missing tokenAddress. ⏭️")
                        continue

                    # Add to unique scanned sets
                    self.unique_tokens_scanned_today.add(base_token_address)
                    self.total_unique_tokens_ever.add(base_token_address)

                    base_token_address_short = _shorten_address(base_token_address)
                    token_symbol_from_profile = token_profile.get('symbol') or token_profile.get('name') or "UnknownSymbol"
                    phase1_discovery_time = datetime.now()

                    log_prefix_phase1 = f"✨ Token {token_symbol_from_profile} (Mint: {base_token_address_short}) (Phase 1)"
                    logger.info(f"{log_prefix_phase1} Discovered at {phase1_discovery_time}.")

                    if FILTER_FOR_PUMPFUN_ONLY and PUMPFUN_ADDRESS_SUFFIX:
                        if not base_token_address.endswith(PUMPFUN_ADDRESS_SUFFIX):
                            logger.info(f"{log_prefix_phase1} ❌ Skipped: Mint '{base_token_address}' does not match Pump.fun suffix '{PUMPFUN_ADDRESS_SUFFIX}'. ⏭️")
                            continue
                        else:
                            logger.info(f"{log_prefix_phase1} ✅ Passed Pump.fun suffix filter.")
                    elif FILTER_FOR_PUMPFUN_ONLY and not PUMPFUN_ADDRESS_SUFFIX:
                        logger.warning(f"{log_prefix_phase1} ⚠️ Pump.fun filtering enabled but PUMPFUN_ADDRESS_SUFFIX is not set.")

                    logger.info(f"{log_prefix_phase1} Starting Phase 2: Detailed Metrics Fetching.")
                    pair_data = None
                    dex_search_base_url = DEXSCREENER_SEARCH_API
                    search_url = f"{dex_search_base_url}/search" if dex_search_base_url else "https://api.dexscreener.com/latest/dex/search"

                    queries = []
                    if token_symbol_from_profile != 'UnknownSymbol':
                        queries.append(f"{token_symbol_from_profile}/SOL")
                        queries.append(f"{token_symbol_from_profile}/USDC")
                    queries.append(base_token_address)

                    for q_idx, q_value in enumerate(queries):
                        logger.info(f"{log_prefix_phase1} 🔎 Phase 2: Attempting search with q='{q_value}' (Attempt {q_idx+1}/{len(queries)})")
                        try:
                            async with self.session.get(search_url, params={'q': q_value}) as search_response:
                                search_response.raise_for_status()
                                search_data = await search_response.json()
                                if search_data and search_data.get('pairs'):
                                    relevant_pairs = []
                                    for p in search_data['pairs']:
                                        if p.get('baseToken', {}).get('address') == base_token_address and \
                                           p.get('quoteToken', {}).get('symbol', '').upper() in ['SOL', 'USDC', 'USDT']:
                                            relevant_pairs.append(p)
                                    if relevant_pairs:
                                        pair_data = relevant_pairs[0] # Simplistic: take first relevant
                                        pair_address_short = _shorten_address(pair_data.get('pairAddress', 'N/A'))
                                        logger.info(f"{log_prefix_phase1} 🔗 Phase 2: Successfully fetched pair data for q='{q_value}'. Pair: {pair_address_short}")
                                        break
                                    else: logger.warning(f"{log_prefix_phase1} ⚠️ Phase 2: Search q='{q_value}' yielded pairs, but none matched criteria.")
                                else: logger.warning(f"{log_prefix_phase1} ⚠️ Phase 2: Search q='{q_value}' returned no pairs.")
                        except aiohttp.ClientResponseError as e: logger.error(f"{log_prefix_phase1}  Phase 2: HTTP error for q='{q_value}': {e.status} - {e.message}")
                        except Exception as e: logger.error(f"{log_prefix_phase1} Phase 2: Unexpected error for q='{q_value}': {e}", exc_info=True)
                        if pair_data: break

                    if not pair_data:
                        logger.warning(f"{log_prefix_phase1} ⚠️ Phase 2: Failed to fetch detailed metrics after all attempts. Skipping. ⏭️")
                        continue

                    # --- Phase 3 ---
                    actual_pair_symbol = pair_data.get('baseToken',{}).get('symbol') or token_symbol_from_profile
                    short_pair_address_for_log = _shorten_address(pair_data.get('pairAddress','N/A'))
                    log_prefix_phase3_context = f"Token ${actual_pair_symbol} (Pair: {short_pair_address_for_log})"

                    passed_phase3_checks, reason = self.analyze_token_metrics(pair_data, base_token_address, token_symbol_from_profile)
                    if not passed_phase3_checks:
                        # analyze_token_metrics now logs its own failures, so just continue
                        continue
                    # analyze_token_metrics logs its own overall success message.

                    logger.info(f"{log_prefix_phase3_context} (Phase 3) 🛡️ Querying RugCheck summary for {base_token_address_short}")
                    if not self.session or self.session.closed: await self.initialize()
                    rugcheck_assessment = await self.verify_token_safety_rugcheck(self.session, base_token_address)
                    log_score_norm = rugcheck_assessment.get('score_normalised', 'N/A')

                    if not rugcheck_assessment.get('is_safe', False):
                        logger.warning(f"{log_prefix_phase3_context} (Phase 3) 👎 RugCheck: Unsafe. ScoreNorm={log_score_norm}. Reasons: {'; '.join(rugcheck_assessment.get('reasons',[])) if rugcheck_assessment.get('reasons') else 'N/A'}. Filtered out. ⏭️")
                        continue
                    logger.info(f"{log_prefix_phase3_context} (Phase 3) 👍 RugCheck: Safe. ScoreNorm={log_score_norm}.")

                    social_sentiment_token_symbol = actual_pair_symbol
                    social_sentiment_data = await self.get_social_sentiment_placeholder(self.session, social_sentiment_token_symbol, base_token_address)
                    logger.info(f"{log_prefix_phase3_context} (Phase 3) 💬 Sentiment (Placeholder): Score='{social_sentiment_data.get('sentiment_score', 'N/A')}', Label='{social_sentiment_data.get('sentiment', 'N/A')}'")

                    price_usd_str = f"${float(pair_data.get('priceUsd', 0)):.6f}" if pair_data.get('priceUsd') else "N/A"
                    token_entry = {
                        'address': base_token_address,
                        'pair_address': pair_data.get('pairAddress'),
                        'symbol': actual_pair_symbol,
                        'timestamp': phase1_discovery_time,
                        'phase1_discovered_at': phase1_discovery_time,
                        'phase2_data_fetched_at': datetime.now(), # TODO: More precise timing if critical
                        'detailed_pair_data': pair_data,
                        'rugcheck_assessment': rugcheck_assessment,
                        'social_sentiment': social_sentiment_data,
                        'api_source': 'token-profiles & dex-search',
                        'log_prefix_for_trade': log_prefix_phase3_context
                    }
                    self.potential_tokens.append(token_entry)
                    logger.info(f"{log_prefix_phase3_context} (Phase 3) 🚀 Added to potential tokens. Price: {price_usd_str}")

        except Exception as e:
            logger.error(f"Error in scan_new_tokens main loop: {e}", exc_info=True)

    def _calculate_buy_sell_ratio(self, detailed_pair_data):
        """Calculates buy/sell ratio from detailed_pair_data (txns.h1)."""
        if not detailed_pair_data: return 0.0
        txns_h1 = detailed_pair_data.get('txns', {}).get('h1', {})
        buys = txns_h1.get('buys', 0)
        sells = txns_h1.get('sells', 0)

        if not (isinstance(buys, (int, float)) and isinstance(sells, (int, float))):
            logger.warning(f"Invalid buy/sell data types: buys={buys}, sells={sells}. Returning 0.0 ratio.")
            return 0.0
        if sells > 0:
            return buys / sells
        elif buys > 0 and sells == 0: # Buyers but no sellers
            return float('inf')
        return 0.0 # No buys or sells in H1

    def get_scanner_metrics(self) -> dict:
        """Returns a dictionary of current scanner metrics, including recent potential tokens."""
        recent_tokens_details = []
        # Get the last 3 tokens, or fewer if not enough are present
        num_to_fetch = min(3, len(self.potential_tokens))

        for token_entry in self.potential_tokens[-num_to_fetch:]:
            detailed_pair_data = token_entry.get('detailed_pair_data', {})

            # Symbol prioritization
            symbol = detailed_pair_data.get('baseToken', {}).get('symbol')
            if not symbol: # Fallback to symbol stored directly in token_entry (from phase 1 profile)
                symbol = token_entry.get('symbol', 'N/A')

            pair_address = token_entry.get('pair_address', 'N/A')
            pair_address_short = _shorten_address(pair_address) if pair_address != 'N/A' else 'N/A'

            price_usd_raw = detailed_pair_data.get('priceUsd')
            price_usd_str = "N/A"
            if price_usd_raw is not None:
                try:
                    price_usd_str = f"{float(price_usd_raw):.6f}" # Format to 6 decimal places
                except (ValueError, TypeError):
                    price_usd_str = str(price_usd_raw) # Keep original if not floatable

            discovered_at_raw = token_entry.get('phase1_discovered_at') or token_entry.get('timestamp')
            discovered_at_iso = "N/A"
            if isinstance(discovered_at_raw, datetime):
                discovered_at_iso = discovered_at_raw.isoformat()
            elif discovered_at_raw is not None: # If it's already a string or other type
                discovered_at_iso = str(discovered_at_raw)

            recent_tokens_details.append({
                "symbol": symbol,
                "pair_address_short": pair_address_short,
                "price_usd": price_usd_str,
                "discovered_at": discovered_at_iso
            })

        return {
            "unique_tokens_scanned_today": len(self.unique_tokens_scanned_today),
            "total_unique_tokens_scanned_all_time": len(self.total_unique_tokens_ever),
            "potential_tokens_count": len(self.potential_tokens),
            "potential_tokens_recent": recent_tokens_details,
            "rugcheck_jwt_status": "Loaded" if self.rugcheck_jwt else "Not Loaded",
            "last_daily_reset_at": self.last_scan_reset_time.isoformat()
        }

    async def verify_token_safety_rugcheck(self, session: aiohttp.ClientSession, token_address: str) -> dict:
        """
        Verifies token safety using the RugCheck.xyz API endpoint (/v1/tokens/{id}/report/summary).
        Uses configuration from config.py for API endpoint, score thresholds, and critical risk names.
        """
        url = f"{RUGCHECK_API_ENDPOINT}/{token_address}/report/summary"
        headers = {"Accept": "application/json"}
        if self.rugcheck_jwt:
            # Assuming self.rugcheck_jwt (whether static or dynamically generated) is a JWT requiring Bearer prefix.
            headers["Authorization"] = f"Bearer {self.rugcheck_jwt}"
            logger.debug(f"Using JWT for RugCheck API request to {url}.")
        else:
            logger.debug(f"No JWT available/configured for RugCheck API request to {url}. Attempting unauthenticated request.")

        default_result = lambda reasons_list, err_msg: {
            'is_safe': False, 'score': None, 'score_normalised': None,
            'risks': [], 'reasons': reasons_list, 'api_error': err_msg
        }

        if not token_address:
            return default_result(['No token address provided.'], 'No token address provided.')

        logger.info(f"Querying RugCheck summary for {token_address}") # URL logged by caller if needed
        try:
            # Use the 'session' argument passed to this method, not self.session directly unless intended
            async with session.get(url, headers=headers, timeout=20) as response:
                raw_response_text = await response.text()
                status_code = response.status

                if status_code == 404: return default_result(['Token not found on RugCheck'], f'Token not found ({status_code})')
                if status_code in [401, 403]: return default_result(['RugCheck API authorization failed'], f'Auth error ({status_code})')
                if status_code == 429: return default_result(['RugCheck API rate limit exceeded'], f'Rate limit ({status_code})')
                response.raise_for_status() # For other 4xx/5xx errors

                response_data = await response.json(content_type=None)
                if not isinstance(response_data, dict):
                    return default_result(['Empty/invalid response from RugCheck'], 'Invalid API response format')

                is_safe = True; reasons = []
                score = response_data.get('score')
                score_normalised_value = response_data.get('scoreNormalised')
                logger.debug(f"RugCheck raw scores for {token_address}: score='{score}', scoreNormalised='{score_normalised_value}'")

                # Score check (Lower scores are better. RUGCHECK_SCORE_THRESHOLD is the max acceptable score.)
                VALID_SCORE_MIN = 0.0
                VALID_SCORE_MAX = 150.0 # Assuming scores don't realistically go much higher

                check_score = None
                selected_score_field_name = None

                # Try scoreNormalised first
                if score_normalised_value is not None:
                    try:
                        temp_score_norm = float(score_normalised_value)
                        if VALID_SCORE_MIN <= temp_score_norm <= VALID_SCORE_MAX:
                            check_score = temp_score_norm
                            selected_score_field_name = "scoreNormalised"
                        else:
                            logger.warning(f"RugCheck: scoreNormalised ('{score_normalised_value}') for {token_address} is outside valid range ({VALID_SCORE_MIN}-{VALID_SCORE_MAX}).")
                    except (ValueError, TypeError):
                        logger.warning(f"RugCheck: scoreNormalised ('{score_normalised_value}') for {token_address} is not a valid number.")

                # If scoreNormalised wasn't valid or used, try 'score'
                if check_score is None and score is not None:
                    try:
                        temp_score = float(score)
                        if VALID_SCORE_MIN <= temp_score <= VALID_SCORE_MAX:
                            check_score = temp_score
                            selected_score_field_name = "score"
                        else:
                            logger.warning(f"RugCheck: score ('{score}') for {token_address} is outside valid range ({VALID_SCORE_MIN}-{VALID_SCORE_MAX}).")
                    except (ValueError, TypeError):
                        logger.warning(f"RugCheck: score ('{score}') for {token_address} is not a valid number.")

                # Now, process check_score
                if check_score is None:
                    is_safe = False # Mark as unsafe if no valid score could be determined
                    reasons.append("No valid score (normalised or raw) available from RugCheck after validation.")
                    logger.warning(f"RugCheck: No valid score found for {token_address} after checking score='{score}' and scoreNormalised='{score_normalised_value}'.")
                else:
                    logger.info(f"RugCheck: Using '{selected_score_field_name}' value {check_score:.2f} for token {token_address} against threshold {RUGCHECK_SCORE_THRESHOLD}.")
                    if check_score > RUGCHECK_SCORE_THRESHOLD:
                        is_safe = False
                        reasons.append(f"Score ({check_score:.2f} from {selected_score_field_name}) is above threshold ({RUGCHECK_SCORE_THRESHOLD}).")

                # Risks field handling (remains the same, processes `is_safe` which might have been set by score check)
                api_risks_raw = response_data.get('risks') # Get raw value first
                api_risks_for_iteration = [] # Default to empty list for iteration logic

                if api_risks_raw is None: # Key was missing entirely
                    logger.info(f"Token {token_address}: 'risks' field missing from RugCheck response. Cannot check for specific critical risk names.")
                    # reasons.append("Risk details list (risks field) missing from RugCheck response.") # Optional, can make reasons noisy
                elif not isinstance(api_risks_raw, list): # Present but not a list
                    is_safe = False # Mark unsafe due to malformed risk data
                    malformed_reason = "Malformed 'risks' field in RugCheck API response (expected a list)."
                    reasons.append(malformed_reason)
                    logger.warning(f"Token {token_address}: {malformed_reason} Got {type(api_risks_raw).__name__}. Marking unsafe.")
                else: # It's a list, so safe to iterate
                    api_risks_for_iteration = api_risks_raw
                    for risk in api_risks_for_iteration: # Iterate the valid list
                        risk_name = risk.get('name')
                        if risk_name in RUGCHECK_CRITICAL_RISK_NAMES:
                            is_safe = False # This can still be set by a critical risk even if score was good
                            reason_description = f"Critical risk: {risk_name} - {risk.get('description','N/A')}"
                            reasons.append(reason_description)
                            logger.warning(f"Token {token_address}: {reason_description}")

                            if risk_name == "MintAuthorityEnabled":
                                logger.warning(f"Token {token_address}: Mint authority is ENABLED.")
                            elif risk_name == "FreezeAuthorityEnabled":
                                logger.warning(f"Token {token_address}: Freeze authority is ENABLED.")

                return {
                    'is_safe': is_safe, 'score': score, 'score_normalised': score_normalised_value,
                    'risks': api_risks_raw if api_risks_raw is not None else [], # Return original 'risks' value, or empty list if it was None
                    'reasons': reasons, 'api_error': None
                }
        except aiohttp.ClientResponseError as e: return default_result([f"HTTP error: {e.status} - {e.message}"], str(e)) # Include e.message
        except asyncio.TimeoutError: return default_result(["API call timed out"], "Timeout")
        except aiohttp.ContentTypeError as e: return default_result(["JSON decode error"], str(e)) # ContentTypeError might not have a clean .message
        except Exception as e:
            logger.error(f"Unexpected error in verify_token_safety_rugcheck for {token_address}: {e}", exc_info=True)
            return default_result([f"Unexpected error: {str(e)}"], str(e))

    async def get_social_sentiment_placeholder(self, session: aiohttp.ClientSession, token_symbol: str, token_address: str) -> dict:
        """
        Placeholder: Simulates fetching social sentiment for a token.
        (Docstring and implementation remain as per previous successful application)
        """
        await asyncio.sleep(0.1)
        logger.debug(f"Simulating social sentiment check for {token_symbol} ({token_address})")
        return {
            'sentiment_score': 0.5, 'sentiment': 'neutral',
            'posts_analyzed': 0, 'source': 'placeholder'
        }

    def get_potential_tokens(self):
        current_time = datetime.now()
        self.potential_tokens = [
            token for token in self.potential_tokens
            if (current_time - token['timestamp']).total_seconds() < 3600
        ]
        return self.potential_tokens

    async def start_scanning(self):
        await self.initialize()
        while True:
            await self.scan_new_tokens()
            await asyncio.sleep(60)
