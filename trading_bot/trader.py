import asyncio
import logging
from datetime import datetime
import aiohttp
import pandas as pd
import numpy as np
from typing import Dict, List, Optional

from .config import (
    DRY_RUN,
    STOP_LOSS_PERCENTAGE,
    TRAILING_STOP_LOSS_PERCENTAGE,
    TRADER_MAX_POSITION_SIZE,
    TRADER_DEFAULT_TAKE_PROFIT_PCT,
    MACD_FAST_PERIOD, MACD_SLOW_PERIOD, MACD_SIGNAL_PERIOD,
    BOLLINGER_WINDOW, BOLLINGER_STD_DEV
)
from .database import TradeDatabase

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

class SolanaTrader:
    def __init__(self):
        self.session: Optional[aiohttp.ClientSession] = None
        self.db = TradeDatabase()
        self.active_positions: Dict[str, dict] = self.db.load_positions()
        self.position_history: List[dict] = self.db.get_trade_history()
        self.dry_run = DRY_RUN
        self.performance_metrics = self.db.load_metrics()
        self.telegram = None  # Set by main.py after construction
        self._paused = False
        self.max_position_size = TRADER_MAX_POSITION_SIZE
        self.stop_loss_pct = STOP_LOSS_PERCENTAGE / 100.0
        self.take_profit_pct = TRADER_DEFAULT_TAKE_PROFIT_PCT

        self.macd_params = {'fast': MACD_FAST_PERIOD, 'slow': MACD_SLOW_PERIOD, 'signal': MACD_SIGNAL_PERIOD}
        self.bollinger_params = {'window': BOLLINGER_WINDOW, 'num_std': BOLLINGER_STD_DEV}
        self.stoch_params = {'k_window': 14, 'd_window': 3}
        self.roc_window = 12
        self.ichimoku_params = {
            'tenkan': 9,
            'kijun': 26,
            'senkou_span_b': 52,
            'displacement': 26
        }

        mode = "DRY RUN" if self.dry_run else "LIVE"
        logger.info(f"Starting trader in {mode} mode with enhanced monitoring")

    async def initialize_session(self):
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
            logger.info("Trader HTTP session initialized.")

    async def close_session(self):
        if self.session:
            await self.session.close()
            logger.info("Trader HTTP session closed.")
        if self.db:
            self.db.save_metrics(self.performance_metrics)
            self.db.close()

    async def calculate_technical_indicators(self, price_data: List[float], volume_data: List[float]) -> Dict:
        prices = np.array(price_data)
        volumes = np.array(volume_data)

        ma_20 = np.mean(prices[-20:]) if len(prices) >= 20 else np.mean(prices)
        ma_50 = np.mean(prices[-50:]) if len(prices) >= 50 else np.mean(prices)

        diff = np.diff(prices)
        gains = np.where(diff > 0, diff, 0)
        losses = np.where(diff < 0, -diff, 0)
        avg_gain = np.mean(gains[-14:]) if len(gains) >= 14 else np.mean(gains)
        avg_loss = np.mean(losses[-14:]) if len(losses) >= 14 else np.mean(losses)
        rs = avg_gain / avg_loss if avg_loss != 0 else 0
        rsi = 100 - (100 / (1 + rs))

        exp1 = pd.Series(prices).ewm(span=self.macd_params['fast']).mean()
        exp2 = pd.Series(prices).ewm(span=self.macd_params['slow']).mean()
        macd = exp1 - exp2
        signal = macd.ewm(span=self.macd_params['signal']).mean()
        macd_hist = macd - signal

        bb_ma = pd.Series(prices).rolling(window=self.bollinger_params['window']).mean()
        bb_std = pd.Series(prices).rolling(window=self.bollinger_params['window']).std()
        bb_upper = bb_ma + (bb_std * self.bollinger_params['num_std'])
        bb_lower = bb_ma - (bb_std * self.bollinger_params['num_std'])

        low_min = pd.Series(prices).rolling(window=self.stoch_params['k_window']).min()
        high_max = pd.Series(prices).rolling(window=self.stoch_params['k_window']).max()
        k = 100 * (prices[-1] - low_min.iloc[-1]) / (high_max.iloc[-1] - low_min.iloc[-1])
        d = pd.Series([k]).rolling(window=self.stoch_params['d_window']).mean().iloc[-1]

        obv = np.zeros_like(volumes)
        obv[0] = volumes[0]
        for i in range(1, len(volumes)):
            if prices[i] > prices[i-1]:
                obv[i] = obv[i-1] + volumes[i]
            elif prices[i] < prices[i-1]:
                obv[i] = obv[i-1] - volumes[i]
            else:
                obv[i] = obv[i-1]

        roc = ((prices[-1] - prices[-self.roc_window]) / prices[-self.roc_window]) * 100 if len(prices) > self.roc_window else 0

        avg_volume = np.mean(volumes)
        vol_ratio = volumes[-1] / avg_volume if avg_volume != 0 else 1

        ichimoku = await self.calculate_ichimoku(prices)

        result = {
            'ma_20': ma_20,
            'ma_50': ma_50,
            'rsi': rsi,
            'volume_ratio': vol_ratio,
            'macd': macd.iloc[-1],
            'macd_signal': signal.iloc[-1],
            'macd_hist': macd_hist.iloc[-1],
            'bb_upper': bb_upper.iloc[-1],
            'bb_lower': bb_lower.iloc[-1],
            'bb_ma': bb_ma.iloc[-1],
            'stoch_k': k,
            'stoch_d': d,
            'obv': obv[-1],
            'obv_change': obv[-1] - obv[-2] if len(obv) > 1 else 0,
            'roc': roc
        }

        if ichimoku:
            result.update({
                'tenkan_sen': ichimoku['tenkan_sen'],
                'kijun_sen': ichimoku['kijun_sen'],
                'senkou_span_a': ichimoku['senkou_span_a'],
                'senkou_span_b': ichimoku['senkou_span_b'],
                'chikou_span': ichimoku['chikou_span'],
                'cloud_top': ichimoku['cloud_top'],
                'cloud_bottom': ichimoku['cloud_bottom'],
                'cloud_direction': ichimoku['cloud_direction']
            })

        return result

    async def calculate_ichimoku(self, prices: np.ndarray) -> Optional[Dict]:
        try:
            df = pd.DataFrame({'price': prices})

            high_9 = df['price'].rolling(window=self.ichimoku_params['tenkan']).max()
            low_9 = df['price'].rolling(window=self.ichimoku_params['tenkan']).min()
            tenkan_sen = (high_9 + low_9) / 2

            high_26 = df['price'].rolling(window=self.ichimoku_params['kijun']).max()
            low_26 = df['price'].rolling(window=self.ichimoku_params['kijun']).min()
            kijun_sen = (high_26 + low_26) / 2

            senkou_span_a = ((tenkan_sen + kijun_sen) / 2).shift(self.ichimoku_params['displacement'])

            high_52 = df['price'].rolling(window=self.ichimoku_params['senkou_span_b']).max()
            low_52 = df['price'].rolling(window=self.ichimoku_params['senkou_span_b']).min()
            senkou_span_b = ((high_52 + low_52) / 2).shift(self.ichimoku_params['displacement'])

            chikou_span = df['price'].shift(-self.ichimoku_params['displacement'])

            return {
                'tenkan_sen': tenkan_sen.iloc[-1],
                'kijun_sen': kijun_sen.iloc[-1],
                'senkou_span_a': senkou_span_a.iloc[-1],
                'senkou_span_b': senkou_span_b.iloc[-1],
                'chikou_span': chikou_span.iloc[-1] if len(chikou_span.dropna()) > 0 else None,
                'current_price': prices[-1],
                'cloud_top': max(senkou_span_a.iloc[-1], senkou_span_b.iloc[-1]),
                'cloud_bottom': min(senkou_span_a.iloc[-1], senkou_span_b.iloc[-1]),
                'cloud_direction': 1 if senkou_span_a.iloc[-1] > senkou_span_b.iloc[-1] else -1
            }
        except Exception as e:
            logger.error(f"Error calculating Ichimoku Cloud: {e}")
            return None

    async def should_trade(self, token_address: str, rugcheck_data: Optional[dict] = None, sentiment_data: Optional[dict] = None) -> tuple[bool, str]:
        log_prefix = f"Token {token_address}:"

        if rugcheck_data:
            if not rugcheck_data.get('is_safe', False):
                reasons = rugcheck_data.get('reasons', ['No specific reason provided'])
                logger.info(f"{log_prefix} Deemed unsafe by RugCheck. Reasons: {reasons}. Skipping.")
                return False, f"Token unsafe per Rugcheck: {', '.join(reasons)}"
        else:
            logger.warning(f"{log_prefix} No Rugcheck data provided. Proceeding with caution.")

        if rugcheck_data:
            logger.debug(f"{log_prefix} Passed RugCheck screen. Is Safe: {rugcheck_data.get('is_safe')}, "
                         f"Score: {rugcheck_data.get('score_normalised', rugcheck_data.get('score', 'N/A'))}")

        if sentiment_data:
            logger.debug(f"{log_prefix} Sentiment: '{sentiment_data.get('sentiment', 'N/A')}', Score: '{sentiment_data.get('sentiment_score', 'N/A')}'")

        try:
            price_history = await self.get_price_history(token_address)
            volume_history = await self.get_volume_history(token_address)

            if not price_history or not volume_history or len(price_history) < 2 or len(volume_history) < 2:
                return False, "Insufficient historical data"

            indicators = await self.calculate_technical_indicators(price_history, volume_history)
            current_price = price_history[-1]

            signals = {
                'trend': indicators['ma_20'] > indicators['ma_50'],
                'rsi_oversold': indicators['rsi'] < 30,
                'volume_spike': indicators['volume_ratio'] > 1.5,
                'macd_bullish': indicators['macd_hist'] > 0 and indicators['macd'] > indicators['macd_signal'],
                'bb_oversold': current_price < indicators['bb_lower'],
                'stoch_oversold': indicators['stoch_k'] < 20 and indicators['stoch_d'] < 20,
                'obv_increasing': indicators['obv_change'] > 0,
                'roc_positive': indicators['roc'] > 0,
                'price_above_cloud': current_price > indicators.get('cloud_top', current_price - 1),
                'bullish_cloud': indicators.get('cloud_direction', 0) > 0,
                'tenkan_kijun_cross': (indicators.get('tenkan_sen', 0) > indicators.get('kijun_sen', 0) and
                                     indicators.get('tenkan_sen', 0) > indicators.get('cloud_top', 0)),
                'chikou_confirmation': (indicators.get('chikou_span') is not None and
                                     indicators.get('chikou_span', 0) > current_price)
            }

            signal_strength = (sum(signals.values()) / len(signals)) * 100

            primary_signals = (
                signals['trend'] and
                signals['macd_bullish'] and
                signals['volume_spike'] and
                signals['price_above_cloud']
            )

            ichimoku_signals = (
                signals['bullish_cloud'] and
                signals['tenkan_kijun_cross']
            )

            secondary_signals = (
                signals['rsi_oversold'] or
                signals['bb_oversold'] or
                signals['stoch_oversold']
            )

            if primary_signals and ichimoku_signals and secondary_signals:
                return True, f"Strong buy signal ({signal_strength:.1f}% confidence)"

            if primary_signals and (ichimoku_signals or secondary_signals):
                if signal_strength > 65:
                    return True, f"Moderate buy signal ({signal_strength:.1f}% confidence)"

            return False, f"Insufficient signals ({signal_strength:.1f}% confidence)"

        except Exception as e:
            logger.error(f"{log_prefix} Error in trade analysis: {e}", exc_info=True)
            return False, f"Analysis error: {str(e)}"

    async def execute_trade(self, token_address: str, amount: float, is_buy: bool = True, symbol: str = "N/A") -> bool:
        if self.dry_run:
            action = "BUY" if is_buy else "SELL"
            logger.info(f"[DRY RUN] Would {action} {amount} SOL worth of {symbol} ({token_address})")

            if is_buy:
                entry_price = await self.get_token_price(token_address)
                if entry_price is not None and entry_price > 0:
                    self.active_positions[token_address] = {
                        'symbol': symbol,
                        'entry_price': entry_price,
                        'amount': amount,
                        'stop_loss': entry_price * (1 - self.stop_loss_pct),
                        'take_profit': entry_price * (1 + self.take_profit_pct),
                        'highest_price_since_entry': entry_price,
                        'entry_time': datetime.now().isoformat()
                    }
                    self.performance_metrics['executed_trades'] += 1
                    self.db.save_position(token_address, self.active_positions[token_address], self.dry_run)
                    self.db.record_trade(token_address, symbol, 'BUY', amount,
                                        price=entry_price, entry_price=entry_price,
                                        stop_loss=self.active_positions[token_address]['stop_loss'],
                                        take_profit=self.active_positions[token_address]['take_profit'],
                                        dry_run=self.dry_run)
                    logger.info(f"[DRY RUN] Opened position for {symbol} at ${entry_price:.6f}, Amount: {amount} SOL, "
                                f"SL: ${self.active_positions[token_address]['stop_loss']:.6f}, "
                                f"TP: ${self.active_positions[token_address]['take_profit']:.6f}")
                    if self.telegram:
                        await self.telegram.notify_trade("BUY", symbol, token_address, amount, entry_price)
                elif entry_price is None:
                    logger.error(f"Could not obtain entry price for {token_address}. Buy order not placed.")
                else:
                    logger.warning(f"Entry price for {token_address} is {entry_price}. Buy order not placed.")

            else:  # Selling
                position = self.active_positions.get(token_address)
                if position:
                    current_price = await self.get_token_price(token_address)
                    if current_price is not None and current_price > 0:
                        profit_loss = (current_price - position['entry_price']) / position['entry_price'] * 100
                        self.update_metrics(profit_loss)

                        self.position_history.append({
                            'symbol': position.get('symbol', 'N/A'),
                            'address': token_address,
                            'entry_price': position['entry_price'],
                            'exit_price': current_price,
                            'amount': position['amount'],
                            'profit_loss_pct': round(profit_loss, 2),
                            'entry_time': position.get('entry_time', 'N/A'),
                            'exit_time': datetime.now().isoformat()
                        })

                        self.db.record_trade(token_address, position.get('symbol', 'N/A'), 'SELL',
                                             position['amount'], price=current_price,
                                             entry_price=position['entry_price'],
                                             exit_price=current_price,
                                             profit_loss_pct=round(profit_loss, 2),
                                             dry_run=self.dry_run)
                        self.db.remove_position(token_address)
                        logger.info(f"[DRY RUN] Closed position for {position.get('symbol', 'N/A')} at ${current_price:.6f}, "
                                    f"Entry: ${position['entry_price']:.6f}, P/L: {profit_loss:.2f}%")
                        if self.telegram:
                            await self.telegram.notify_trade("SELL", position.get('symbol', 'N/A'),
                                                            token_address, position['amount'],
                                                            current_price, profit_loss)
                        del self.active_positions[token_address]
                    elif current_price is None:
                        logger.error(f"Could not obtain current price for {token_address} to sell.")
                    else:
                        logger.warning(f"Current price for {token_address} is {current_price}. Sell order not executed.")

            return True

        # Live trading - not implemented yet
        logger.warning(f"Live trading not implemented. Set DRY_RUN=true in .env")
        return False

    async def manage_positions(self):
        if not self.session or self.session.closed:
            logger.warning("Session not available in manage_positions. Attempting to initialize.")
            await self.initialize_session()
            if not self.session or self.session.closed:
                logger.error("Failed to ensure session is active in manage_positions.")
                return

        active_position_keys = list(self.active_positions.keys())
        if not active_position_keys:
            return

        logger.info(f"Managing {len(active_position_keys)} active positions...")
        for token_address in active_position_keys:
            if token_address not in self.active_positions:
                continue

            position = self.active_positions[token_address]
            current_price = await self.get_token_price(token_address)

            if current_price is None or current_price <= 0:
                logger.warning(f"Could not get valid price for {token_address} during position management. Skipping.")
                continue

            if token_address not in self.active_positions:
                continue

            # Trailing Stop Loss
            if current_price > position.get('highest_price_since_entry', position['entry_price']):
                position['highest_price_since_entry'] = current_price

            trailing_stop_price = position['highest_price_since_entry'] * (1 - TRAILING_STOP_LOSS_PERCENTAGE / 100.0)
            old_stop_loss = position['stop_loss']
            new_stop_loss = max(old_stop_loss, trailing_stop_price)

            if new_stop_loss > old_stop_loss:
                position['stop_loss'] = new_stop_loss
                self.db.save_position(token_address, position, self.dry_run)
                logger.info(f"Trailing SL for {position.get('symbol', token_address)} updated: "
                            f"${old_stop_loss:.6f} -> ${new_stop_loss:.6f}")

            if current_price <= position['stop_loss']:
                logger.info(f"Stop loss triggered for {position.get('symbol', token_address)} at ${current_price:.6f} "
                            f"(SL: ${position['stop_loss']:.6f})")
                if self.telegram:
                    await self.telegram.notify_stop_loss(position.get('symbol', 'N/A'),
                                                        token_address, current_price, position['stop_loss'])
                await self.execute_trade(token_address, position['amount'], is_buy=False,
                                         symbol=position.get('symbol', 'N/A'))
            elif current_price >= position['take_profit']:
                logger.info(f"Take profit triggered for {position.get('symbol', token_address)} at ${current_price:.6f} "
                            f"(TP: ${position['take_profit']:.6f})")
                if self.telegram:
                    await self.telegram.notify_take_profit(position.get('symbol', 'N/A'),
                                                          token_address, current_price, position['take_profit'])
                await self.execute_trade(token_address, position['amount'], is_buy=False,
                                         symbol=position.get('symbol', 'N/A'))

    async def check_token_contract(self, token_address):
        """Placeholder: Simulate contract verification. Returns True for dry run mode."""
        return True

    async def get_wallet_balance(self):
        """Placeholder: Get simulated wallet balance. Returns 10 SOL for dry run mode."""
        return 10.0

    async def get_token_price(self, token_address: str) -> Optional[float]:
        if not self.session or self.session.closed:
            await self.initialize_session()
            if not self.session:
                 return None

        url = f"https://api.dexscreener.com/latest/dex/tokens/{token_address}"

        try:
            async with self.session.get(url) as response:
                response.raise_for_status()
                data = await response.json(content_type=None)

                if not data:
                    return None

                pairs = data.get('pairs')
                if not pairs or not isinstance(pairs, list) or len(pairs) == 0:
                    return None

                first_pair = pairs[0]
                if not isinstance(first_pair, dict):
                    return None

                price_usd_str = first_pair.get('priceUsd')
                if price_usd_str is None:
                    return None

                return float(price_usd_str)

        except aiohttp.ClientResponseError as http_err:
            logger.error(f"HTTP error fetching price for {token_address}: {http_err}")
            return None
        except (aiohttp.ContentTypeError, ValueError, TypeError) as e:
            logger.error(f"Error parsing price for {token_address}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error fetching price for {token_address}: {e}", exc_info=True)
            return None

    async def get_price_history(self, token_address):
        """Placeholder: Returns mock price data. Must be replaced for live trading."""
        return [100.0, 102.0, 101.5, 103.0, 102.5, 104.0, 105.0, 103.5, 106.0, 107.0,
                105.0, 104.5, 106.5, 108.0, 107.0, 109.0, 110.0, 108.5, 109.5, 112.0]

    async def get_volume_history(self, token_address):
        """Placeholder: Returns mock volume data. Must be replaced for live trading."""
        return [1000.0, 1200.0, 1100.0, 1300.0, 1400.0, 1500.0, 1350.0, 1600.0, 1700.0, 1550.0,
                1450.0, 1650.0, 1800.0, 1750.0, 1900.0, 2000.0, 1850.0, 1950.0, 2200.0, 2100.0]

    async def start_trading(self, scanner):
        mode = "DRY RUN" if self.dry_run else "LIVE"
        logger.info(f"Starting trading bot in {mode} mode...")
        await self.initialize_session()

        try:
            while True:
                try:
                    if self._paused:
                        logger.info("Trading paused via Telegram. Waiting...")
                        await asyncio.sleep(10)
                        continue

                    self.performance_metrics['scans'] += 1

                    if scanner and hasattr(scanner, 'get_potential_tokens') and callable(scanner.get_potential_tokens):
                        potential_tokens = scanner.get_potential_tokens()
                    else:
                        logger.error("Scanner not available. Cannot proceed.")
                        await asyncio.sleep(60)
                        continue

                    if potential_tokens:
                        logger.info(f"Received {len(potential_tokens)} potential tokens from scanner.")
                        self.performance_metrics['potential_trades'] = len(potential_tokens)

                        for token_data in potential_tokens:
                            if not isinstance(token_data, dict):
                                continue

                            token_address = token_data.get('address')
                            token_symbol = token_data.get('symbol', 'N/A')

                            if not token_address:
                                continue

                            rugcheck_assessment = token_data.get('rugcheck_assessment', {'is_safe': False})
                            social_sentiment = token_data.get('social_sentiment', {'sentiment': 'N/A'})

                            logger.info(
                                f"Processing: {token_symbol} ({token_address}) - "
                                f"RugCheck Safe: {rugcheck_assessment.get('is_safe')}, "
                                f"Sentiment: {social_sentiment.get('sentiment', 'N/A')}"
                            )

                            if token_address not in self.active_positions:
                                if await self.check_token_contract(token_address):
                                    should_trade_flag, reason = await self.should_trade(
                                        token_address,
                                        rugcheck_data=rugcheck_assessment,
                                        sentiment_data=social_sentiment
                                    )
                                    if should_trade_flag:
                                        price_str = token_data.get('detailed_pair_data', {}).get('priceUsd')
                                        try:
                                            current_price = float(price_str) if price_str else None
                                        except (ValueError, TypeError):
                                            current_price = None

                                        if not current_price or current_price <= 0:
                                            logger.warning(f"Invalid price for {token_symbol}. Skipping trade.")
                                            continue

                                        position_size = await self.calculate_position_size(current_price, 0.5)

                                        if position_size > 0:
                                            logger.info(f"Executing buy for {token_symbol}, Size: {position_size} SOL, Reason: {reason}")
                                            await self.execute_trade(token_address, position_size, is_buy=True, symbol=token_symbol)
                                    else:
                                        logger.info(f"{token_symbol} not traded: {reason}")
                    else:
                        logger.info("No potential tokens from scanner this cycle.")

                    if self.active_positions:
                        await self.manage_positions()

                    if self.performance_metrics['scans'] % 5 == 0:
                        self.log_metrics()

                    await asyncio.sleep(60)

                except asyncio.CancelledError:
                    logger.info("Trading loop cancelled. Shutting down...")
                    break
                except Exception as e:
                    logger.error(f"Error in trading loop: {e}", exc_info=True)
                    await asyncio.sleep(30)
        finally:
            await self.close_session()

    def log_metrics(self):
        metrics = self.performance_metrics
        mode = "DRY RUN" if self.dry_run else "LIVE"
        logger.info(f"\n=== Performance Metrics ({mode}) ===")
        logger.info(f"Total Scans: {metrics['scans']}")
        logger.info(f"Potential Trades Found: {metrics['potential_trades']}")
        logger.info(f"Executed Trades: {metrics['executed_trades']}")
        logger.info(f"Success Rate: {metrics['win_rate']:.1f}%")
        logger.info(f"Total P/L: {metrics['total_profit_loss']:.2f}%")
        logger.info(f"Active Positions: {len(self.active_positions)}")
        logger.info("========================\n")

    def update_metrics(self, trade_result=None):
        if trade_result is not None:
            if trade_result > 0:
                self.performance_metrics['successful_trades'] += 1
            else:
                self.performance_metrics['failed_trades'] += 1

            self.performance_metrics['total_profit_loss'] += trade_result
            total_closed = self.performance_metrics['successful_trades'] + self.performance_metrics['failed_trades']
            if total_closed > 0:
                self.performance_metrics['win_rate'] = (
                    self.performance_metrics['successful_trades'] / total_closed * 100
                )
            self.db.save_metrics(self.performance_metrics)

    async def calculate_position_size(self, token_price: float, volatility: float) -> float:
        balance = await self.get_wallet_balance()
        vol_factor = 1 - min(volatility, 0.5)
        base_size = balance * self.max_position_size
        adjusted_size = base_size * vol_factor
        return min(adjusted_size, balance * self.max_position_size)
