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

class TokenScanner:
    def __init__(self):
        self.session: Optional[aiohttp.ClientSession] = None
        self.potential_tokens = []
        self.scan_count = 0
        # Initialize with a static JWT from config if provided.
        # This will be overwritten by a dynamically generated JWT if dynamic generation is successful.
        self.rugcheck_jwt: Optional[str] = STATIC_RUGCHECK_JWT
        self.rugcheck_jwt_generation_attempted: bool = False

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
        pair_base_token_symbol = detailed_pair_data.get('baseToken', {}).get('symbol', 'UnknownSymbol')
        pair_address = detailed_pair_data.get('pairAddress', 'UnknownPairAddr')
        # Use token_address_from_profile (mint address) for primary identification in logs
        log_prefix = f"Token {token_symbol_from_profile or pair_base_token_symbol} (Mint: {token_address_from_profile or 'N/A'}, Pair: {pair_address}) (Phase 3):"

        try:
            if not detailed_pair_data:
                return False, "No detailed pair data provided"

            logger.info(f"{log_prefix} Starting Phase 3 metric analysis.")

            # Market Cap (FDV) Check
            fdv_data = detailed_pair_data.get('fdv')
            if fdv_data is None: return False, f"{log_prefix} Missing FDV (market cap)"
            try: market_cap = float(fdv_data)
            except (ValueError, TypeError): return False, f"{log_prefix} Invalid FDV data type: {fdv_data}"
            if market_cap < TARGET_MARKET_CAP_TO_SCAN:
                return False, f"{log_prefix} MC ${market_cap:,.0f} < Target ${TARGET_MARKET_CAP_TO_SCAN:,.0f}"
            if market_cap > MAX_MARKET_CAP:
                return False, f"{log_prefix} MC ${market_cap:,.0f} > Max ${MAX_MARKET_CAP:,.0f}"
            logger.info(f"{log_prefix} MC Passed: Target ${TARGET_MARKET_CAP_TO_SCAN:,.0f} <= Actual ${market_cap:,.0f} <= Max ${MAX_MARKET_CAP:,.0f}")

            # Token Age (Pair Creation Time) Check
            created_timestamp = detailed_pair_data.get('pairCreatedAt')
            if created_timestamp is None: return False, f"{log_prefix} Missing pairCreatedAt"
            try: created_at = datetime.fromtimestamp(created_timestamp / 1000) # Assuming ms timestamp
            except (TypeError, ValueError): return False, f"{log_prefix} Invalid pairCreatedAt timestamp: {created_timestamp}"
            age_hours = (datetime.now() - created_at).total_seconds() / 3600
            if age_hours > MAX_TOKEN_AGE_HOURS:
                return False, f"{log_prefix} Token too old: {age_hours:.1f}h > {MAX_TOKEN_AGE_HOURS}h"
            logger.info(f"{log_prefix} Age Passed: {age_hours:.1f}h <= {MAX_TOKEN_AGE_HOURS}h")

            # Liquidity Check
            liquidity_data = detailed_pair_data.get('liquidity')
            if not liquidity_data or liquidity_data.get('usd') is None: return False, f"{log_prefix} Missing liquidity USD"
            try: liquidity_usd = float(liquidity_data['usd'])
            except ValueError: return False, f"{log_prefix} Invalid liquidity USD type: {liquidity_data['usd']}"
            if liquidity_usd < MIN_LIQUIDITY:
                return False, f"{log_prefix} Liquidity ${liquidity_usd:,.0f} < Min ${MIN_LIQUIDITY:,.0f}"
            logger.info(f"{log_prefix} Liquidity Passed: ${liquidity_usd:,.0f} >= ${MIN_LIQUIDITY:,.0f}")

            # Transactions Check
            txns_h1_data = detailed_pair_data.get('txns', {}).get('h1', {})
            buys = txns_h1_data.get('buys', 0)
            sells = txns_h1_data.get('sells', 0)
            if (buys + sells) < MIN_TRANSACTIONS:
                return False, f"{log_prefix} Txns (1h) {buys+sells} < Min {MIN_TRANSACTIONS}"
            logger.info(f"{log_prefix} Transaction Count Passed: {buys+sells} >= {MIN_TRANSACTIONS}")

            current_buy_sell_ratio = self._calculate_buy_sell_ratio(detailed_pair_data) # Use the updated method
            if sells > 0 and current_buy_sell_ratio < MIN_BUY_SELL_RATIO: # Check sells > 0 to avoid division by zero if ratio is inf
                return False, f"{log_prefix} Buy/Sell ratio {current_buy_sell_ratio:.2f} < Min {MIN_BUY_SELL_RATIO}"
            logger.info(f"{log_prefix} Buy/Sell Ratio Passed: {current_buy_sell_ratio:.2f} >= {MIN_BUY_SELL_RATIO}")

            # Volume Spike Check
            volume_data = detailed_pair_data.get('volume', {})
            try:
                volume_1h = float(volume_data.get('h1', 0.0))
                volume_24h = float(volume_data.get('h24', 0.0))
            except ValueError: return False, f"{log_prefix} Invalid volume data type. H1: {volume_data.get('h1')}, H24: {volume_data.get('h24')}"

            if VOLUME_SPIKE_THRESHOLD > 0: # Only check if threshold is set
                if volume_24h > 0 and volume_1h > 0 : # Avoid division by zero or meaningless spike calc
                    hourly_equiv_from_24h = volume_24h / 24
                    if hourly_equiv_from_24h == 0: # Avoid division by zero if 24h vol is tiny
                         if volume_1h > 0: # Any 1h volume is a spike if 24h avg is zero
                            logger.info(f"{log_prefix} Volume Spike Passed (1h vol > 0, 24h avg vol = 0).")
                         else: # No volume at all
                            return False, f"{log_prefix} No volume spike: 1h vol is 0 and 24h avg vol is 0."
                    elif (volume_1h / hourly_equiv_from_24h) < VOLUME_SPIKE_THRESHOLD:
                        return False, f"{log_prefix} No volume spike: (1h/avg_1h_from_24h) {(volume_1h / hourly_equiv_from_24h):.1f}x < {VOLUME_SPIKE_THRESHOLD}x"
                    else:
                        logger.info(f"{log_prefix} Volume Spike Passed: {(volume_1h / hourly_equiv_from_24h):.1f}x >= {VOLUME_SPIKE_THRESHOLD}x")
                elif volume_1h > 0 and volume_24h == 0: # 1h volume exists, but no 24h volume, considered a spike
                     logger.info(f"{log_prefix} Volume Spike Passed (1h vol > 0, 24h vol = 0).")
                else: # No 1h volume, or threshold is zero
                    return False, f"{log_prefix} No volume spike: 1h vol is {volume_1h}, 24h vol is {volume_24h}. Threshold: {VOLUME_SPIKE_THRESHOLD}x"
            else:
                logger.info(f"{log_prefix} Volume spike check skipped as threshold is 0.")


            # Price Change Check (1h)
            price_change_1h = float(detailed_pair_data.get('priceChange', {}).get('h1', 0.0))
            # Example: if price_change_1h < -10 (ALLOW_NEGATIVE_PRICE_CHANGE_PERCENTAGE could be a config)
            # For now, let's assume we don't want significant drops.
            if price_change_1h < -25: # Example threshold: filter if price dropped more than 25% in 1hr
                return False, f"{log_prefix} Price drop (1h): {price_change_1h}% < -25%"
            logger.info(f"{log_prefix} Price Change (1h) Passed: {price_change_1h}% >= -25%")

            logger.info(f"{log_prefix} All Phase 3 metric checks passed.")
            return True, "Token passed all Phase 3 checks"
        except Exception as e:
            logger.error(f"{log_prefix} Error in analyze_token_metrics (Phase 3): {e}", exc_info=True)
            return False, str(e)

    async def scan_new_tokens(self):
        """Phase 1 & 2 & 3: Discover, Fetch Details, and Analyze Tokens."""
        try:
            # Use DEXSCREENER_TOKEN_PROFILES_API for Phase 1
            params = {"chainId": "solana"}
            logger.info(f"Phase 1: Starting scan for new token profiles using {DEXSCREENER_TOKEN_PROFILES_API} with params {params}")
            async with self.session.get(DEXSCREENER_TOKEN_PROFILES_API, params=params) as response:
                response.raise_for_status()
                token_profiles = await response.json()
                self.scan_count += 1
                logger.info(f"Phase 1 Scan Iteration {self.scan_count}: Discovered {len(token_profiles)} token profiles.")

                for token_profile in token_profiles:
                    # Extract basic identifiers
                    token_address = token_profile.get('tokenAddress')
                    if not token_address:
                        logger.debug("Phase 1: Skipping profile due to missing tokenAddress.")
                        continue

                    token_symbol = token_profile.get('symbol', token_profile.get('name', 'UnknownSymbol'))
                    phase1_discovery_time = datetime.now() # Record Phase 1 discovery time
                    # Using token_address (mint address) for primary identification in logs
                    log_prefix_phase1 = f"Token {token_symbol} (Mint: {token_address}) (Phase 1):"
                    logger.info(f"{log_prefix_phase1} Discovered at {phase1_discovery_time}.")

                    # Pump.fun Suffix Filter (applied BEFORE Phase 2 API calls)
                    if FILTER_FOR_PUMPFUN_ONLY and PUMPFUN_ADDRESS_SUFFIX:
                        if not token_address.endswith(PUMPFUN_ADDRESS_SUFFIX):
                            logger.debug(f"{log_prefix_phase1} Skipped (Pump.fun suffix filter): Address '{token_address}' does not match suffix '{PUMPFUN_ADDRESS_SUFFIX}'.")
                            continue
                        else:
                            logger.info(f"{log_prefix_phase1} Passed Pump.fun suffix filter. Address '{token_address}' matches suffix '{PUMPFUN_ADDRESS_SUFFIX}'.")
                    elif FILTER_FOR_PUMPFUN_ONLY and not PUMPFUN_ADDRESS_SUFFIX:
                        logger.warning(f"{log_prefix_phase1} Pump.fun filtering is enabled but PUMPFUN_ADDRESS_SUFFIX is not set. No suffix filtering applied.")

                    # Phase 1 conceptual check (already minimal, mainly for structure)
                    # In reality, the old analyze_token_metrics for Phase 1 was just basic validation.
                    # We can consider it passed if we reached here after suffix filter.
                    logger.info(f"{log_prefix_phase1} Conceptually passed Phase 1 (discovery and initial filtering).")

                    # --- Phase 2: Detailed Metrics Fetching ---
                    logger.info(f"{log_prefix_phase1} Starting Phase 2: Detailed Metrics Fetching.")
                    pair_data = None
                    # Use DEXSCREENER_SEARCH_API for Phase 2, append /search path
                    dex_search_base_url = DEXSCREENER_SEARCH_API
                    search_url = f"{dex_search_base_url}/search" if dex_search_base_url else "https://api.dexscreener.com/latest/dex/search" # Fallback if not configured

                    # Querying strategies
                    queries = []
                    if token_symbol and token_symbol != 'UnknownSymbol':
                        queries.append(f"{token_symbol}/SOL")
                        queries.append(f"{token_symbol}/USDC")
                    queries.append(token_address) # Fallback to token address

                    for q_idx, q_value in enumerate(queries):
                        # Use log_prefix_phase1 here as pair_data specific log_prefix (log_prefix_phase3) isn't available yet
                        logger.info(f"{log_prefix_phase1} Phase 2: Attempting search with q='{q_value}' (Attempt {q_idx+1}/{len(queries)}) to {search_url}")
                        try:
                            async with self.session.get(search_url, params={'q': q_value}) as search_response:
                                search_response.raise_for_status()
                                search_data = await search_response.json()

                                if search_data and search_data.get('pairs'):
                                    relevant_pairs = []
                                    for p in search_data['pairs']:
                                        base = p.get('baseToken', {}).get('address')
                                        quote_sym = p.get('quoteToken', {}).get('symbol', '').upper()

                                        if base == token_address and quote_sym in ['SOL', 'USDC', 'USDT']:
                                            relevant_pairs.append(p)

                                    if relevant_pairs:
                                        # Simplistic choice: first relevant. Could be sorted by liquidity.
                                        pair_data = relevant_pairs[0]
                                        logger.info(f"{log_prefix_phase1} Phase 2: Successfully fetched pair data for q='{q_value}'. Pair: {pair_data.get('pairAddress')}")
                                        break
                                    else:
                                        logger.info(f"{log_prefix_phase1} Phase 2: Search with q='{q_value}' yielded pairs, but none directly matched criteria.")
                                else:
                                    logger.info(f"{log_prefix_phase1} Phase 2: Search with q='{q_value}' returned no pairs.")
                        except aiohttp.ClientResponseError as e:
                            logger.error(f"{log_prefix_phase1} Phase 2: HTTP error with q='{q_value}': {e.status} - {e.message}")
                        except Exception as e:
                            logger.error(f"{log_prefix_phase1} Phase 2: Unexpected error with q='{q_value}': {e}", exc_info=True)

                        if pair_data: # If pair_data was found in this attempt, no need to try other queries
                            break

                    if not pair_data:
                        logger.warning(f"{log_prefix} Phase 2: Failed to fetch detailed metrics for token after all query attempts. Skipping.")
                        # Optionally, add a placeholder or flag to potential_tokens if you still want to keep it
                        # For now, we skip adding it if detailed metrics fail.
                        continue

                    # --- End of Phase 2 ---

                    # Update log_prefix for Phase 3 using pair_data if available, fallback to phase 1 info
                    log_prefix_phase3 = f"Token {pair_data.get('baseToken',{}).get('symbol', token_symbol)} (Mint: {token_address}, Pair: {pair_data.get('pairAddress','N/A')}) (Phase 3):"

                    # --- Phase 3: Metric Analysis ---
                    passed_phase3_checks, reason = self.analyze_token_metrics(pair_data, token_address, token_symbol)
                    if not passed_phase3_checks:
                        logger.info(f"{log_prefix_phase3} Failed Phase 3 metric checks: {reason}")
                        continue
                    logger.info(f"{log_prefix_phase3} Passed Phase 3 metric checks.")

                    # Existing RugCheck and Social Sentiment
                    if not self.session or self.session.closed: await self.initialize() # Ensure session
                    rugcheck_assessment = await self.verify_token_safety_rugcheck(self.session, token_address) # Use mint address for RugCheck
                    log_score_norm = rugcheck_assessment.get('score_normalised', 'N/A')
                    # Use consistent log prefix for these subsequent checks
                    logger.info(f"{log_prefix_phase3} RugCheck: Safe={rugcheck_assessment.get('is_safe')}, ScoreNorm={log_score_norm}, Reasons={'; '.join(rugcheck_assessment.get('reasons',[])) if rugcheck_assessment.get('reasons') else 'N/A'}, APIError='{rugcheck_assessment.get('api_error')}'")

                    if not rugcheck_assessment.get('is_safe', False):
                        logger.info(f"{log_prefix_phase3} Filtered out by RugCheck. Reasons: {'; '.join(rugcheck_assessment.get('reasons', ['No specific reasons given']))}")
                        continue
                    logger.info(f"{log_prefix_phase3} Passed RugCheck safety screen.")

                    # Use pair's base token symbol for social sentiment if available, else fallback to profile's symbol
                    social_sentiment_token_symbol = pair_data.get('baseToken',{}).get('symbol', token_symbol)
                    social_sentiment_data = await self.get_social_sentiment_placeholder(self.session, social_sentiment_token_symbol, token_address)
                    logger.info(f"{log_prefix_phase3} Sentiment (Placeholder): Score='{social_sentiment_data.get('sentiment_score', 'N/A')}', Label='{social_sentiment_data.get('sentiment', 'N/A')}'")

                    # Add to potential_tokens list
                    token_entry = {
                        'address': token_address, # Mint address
                        'pair_address': pair_data.get('pairAddress'),
                        'symbol': pair_data.get('baseToken',{}).get('symbol', token_symbol), # Prefer symbol from pair data
                        'timestamp': phase1_discovery_time,
                        'phase1_discovered_at': phase1_discovery_time,
                        'phase2_data_fetched_at': datetime.now(), # This should ideally be set when pair_data is confirmed
                        'detailed_pair_data': pair_data,
                        'rugcheck_assessment': rugcheck_assessment,
                        'social_sentiment': social_sentiment_data,
                        'api_source': 'token-profiles & dex-search',
                        'log_prefix_for_trade': log_prefix_phase3 # Store a consistent log prefix for trading
                    }
                    # Correcting phase2_data_fetched_at: This should be captured right after pair_data is confirmed.
                    # For simplicity in this diff, we'll use current time. A more precise way would be to set it right after `pair_data = relevant_pairs[0]`.

                    self.potential_tokens.append(token_entry)
                    logger.info(f"{log_prefix_phase3} Token passed all checks and added to potential tokens list.")

        except Exception as e:
            logger.error(f"Error in scan_new_tokens (Phases 1-3): {e}", exc_info=True)

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

                # Score check (RUGCHECK_SCORE_THRESHOLD is min acceptable, higher is better for score_normalised)
                check_score = score_normalised_value if score_normalised_value is not None else score
                if check_score is None:
                    is_safe = False
                    reasons.append("Score (normalised or raw) missing from RugCheck.")
                elif not isinstance(check_score, (int, float)):
                    is_safe = False
                    reasons.append(f"Score ({check_score}) from RugCheck is not numeric.")
                elif check_score > RUGCHECK_SCORE_THRESHOLD: # Changed from < to >
                    is_safe = False
                    reasons.append(f"Score ({check_score}) is above threshold ({RUGCHECK_SCORE_THRESHOLD}).") # Updated reason

                # Risks field handling
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
