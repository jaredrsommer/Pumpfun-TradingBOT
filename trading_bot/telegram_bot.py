import asyncio
import logging
from datetime import datetime
from typing import Optional
import aiohttp

from .config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID

logger = logging.getLogger(__name__)


class TelegramBot:
    def __init__(self, trader=None, scanner=None):
        self.token = TELEGRAM_BOT_TOKEN
        self.chat_id = TELEGRAM_CHAT_ID
        self.trader = trader
        self.scanner = scanner
        self.session: Optional[aiohttp.ClientSession] = None
        self.enabled = bool(self.token and self.chat_id)
        self._last_update_id = 0

        if self.enabled:
            logger.info("Telegram bot enabled.")
        else:
            logger.info("Telegram bot disabled (TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID not set).")

    @property
    def api_url(self):
        return f"https://api.telegram.org/bot{self.token}"

    async def initialize(self):
        if not self.enabled:
            return
        self.session = aiohttp.ClientSession()

    async def close(self):
        if self.session:
            await self.session.close()

    async def send_message(self, text: str, chat_id: str = None):
        if not self.enabled or not self.session:
            return
        target = chat_id or self.chat_id
        url = f"{self.api_url}/sendMessage"
        payload = {"chat_id": target, "text": text, "parse_mode": "HTML"}
        try:
            async with self.session.post(url, json=payload) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    logger.error(f"Telegram send failed ({resp.status}): {body}")
        except Exception as e:
            logger.error(f"Telegram send error: {e}")

    async def notify_trade(self, action: str, symbol: str, token_address: str,
                           amount: float, price: float, profit_loss: float = None):
        if not self.enabled:
            return
        emoji = "🟢" if action == "BUY" else "🔴"
        dry = " [DRY RUN]" if self.trader and self.trader.dry_run else ""
        msg = f"{emoji} <b>{action}{dry}</b>\n"
        msg += f"Token: {symbol}\n"
        msg += f"Address: <code>{token_address[:8]}...{token_address[-4:]}</code>\n"
        msg += f"Amount: {amount:.4f} SOL\n"
        msg += f"Price: ${price:.6f}\n"
        if profit_loss is not None:
            pl_emoji = "📈" if profit_loss >= 0 else "📉"
            msg += f"P/L: {pl_emoji} {profit_loss:.2f}%\n"
        msg += f"Time: {datetime.now().strftime('%H:%M:%S')}"
        await self.send_message(msg)

    async def notify_stop_loss(self, symbol: str, token_address: str, price: float, stop_loss: float):
        if not self.enabled:
            return
        msg = f"🛑 <b>STOP LOSS TRIGGERED</b>\n"
        msg += f"Token: {symbol}\n"
        msg += f"Price: ${price:.6f} (SL: ${stop_loss:.6f})"
        await self.send_message(msg)

    async def notify_take_profit(self, symbol: str, token_address: str, price: float, take_profit: float):
        if not self.enabled:
            return
        msg = f"🎯 <b>TAKE PROFIT HIT</b>\n"
        msg += f"Token: {symbol}\n"
        msg += f"Price: ${price:.6f} (TP: ${take_profit:.6f})"
        await self.send_message(msg)

    async def _get_updates(self):
        if not self.enabled or not self.session:
            return []
        url = f"{self.api_url}/getUpdates"
        params = {"offset": self._last_update_id + 1, "timeout": 5}
        try:
            async with self.session.get(url, params=params) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get("result", [])
        except Exception as e:
            logger.error(f"Telegram getUpdates error: {e}")
        return []

    async def _handle_command(self, text: str, chat_id: str):
        cmd = text.strip().lower()

        if cmd == "/status":
            await self._cmd_status(chat_id)
        elif cmd == "/positions":
            await self._cmd_positions(chat_id)
        elif cmd == "/metrics":
            await self._cmd_metrics(chat_id)
        elif cmd == "/history":
            await self._cmd_history(chat_id)
        elif cmd == "/pause":
            await self._cmd_pause(chat_id)
        elif cmd == "/resume":
            await self._cmd_resume(chat_id)
        elif cmd == "/help":
            await self._cmd_help(chat_id)
        else:
            await self.send_message("Unknown command. Use /help for available commands.", chat_id)

    async def _cmd_status(self, chat_id: str):
        if not self.trader:
            await self.send_message("Trader not available.", chat_id)
            return
        mode = "DRY RUN" if self.trader.dry_run else "LIVE"
        m = self.trader.performance_metrics
        msg = f"📊 <b>Bot Status ({mode})</b>\n\n"
        msg += f"Scans: {m.get('scans', 0)}\n"
        msg += f"Executed Trades: {m.get('executed_trades', 0)}\n"
        msg += f"Win Rate: {m.get('win_rate', 0):.1f}%\n"
        msg += f"Total P/L: {m.get('total_profit_loss', 0):.2f}%\n"
        msg += f"Active Positions: {len(self.trader.active_positions)}\n"
        if self.scanner:
            msg += f"Tokens Scanned Today: {len(self.scanner.unique_tokens_scanned_today)}"
        await self.send_message(msg, chat_id)

    async def _cmd_positions(self, chat_id: str):
        if not self.trader or not self.trader.active_positions:
            await self.send_message("No active positions.", chat_id)
            return
        msg = "📋 <b>Active Positions</b>\n\n"
        for addr, pos in self.trader.active_positions.items():
            current_price = await self.trader.get_token_price(addr)
            entry = pos['entry_price']
            pl = ((current_price - entry) / entry * 100) if current_price and entry > 0 else 0
            pl_emoji = "📈" if pl >= 0 else "📉"
            msg += f"<b>{pos.get('symbol', 'N/A')}</b>\n"
            msg += f"  Entry: ${entry:.6f}\n"
            msg += f"  Current: ${current_price:.6f}\n" if current_price else "  Current: N/A\n"
            msg += f"  P/L: {pl_emoji} {pl:.2f}%\n"
            msg += f"  SL: ${pos.get('stop_loss', 0):.6f} | TP: ${pos.get('take_profit', 0):.6f}\n\n"
        await self.send_message(msg, chat_id)

    async def _cmd_metrics(self, chat_id: str):
        if not self.trader:
            await self.send_message("Trader not available.", chat_id)
            return
        m = self.trader.performance_metrics
        msg = "📈 <b>Performance Metrics</b>\n\n"
        msg += f"Total Scans: {m.get('scans', 0)}\n"
        msg += f"Potential Trades: {m.get('potential_trades', 0)}\n"
        msg += f"Executed Trades: {m.get('executed_trades', 0)}\n"
        msg += f"Successful: {m.get('successful_trades', 0)}\n"
        msg += f"Failed: {m.get('failed_trades', 0)}\n"
        msg += f"Win Rate: {m.get('win_rate', 0):.1f}%\n"
        msg += f"Total P/L: {m.get('total_profit_loss', 0):.2f}%"
        await self.send_message(msg, chat_id)

    async def _cmd_history(self, chat_id: str):
        if not self.trader or not self.trader.position_history:
            await self.send_message("No trade history.", chat_id)
            return
        msg = "📜 <b>Recent Trade History</b>\n\n"
        for trade in self.trader.position_history[-5:]:
            pl = trade.get('profit_loss_pct', 0)
            pl_emoji = "✅" if pl >= 0 else "❌"
            msg += f"{pl_emoji} {trade.get('symbol', 'N/A')}: {pl:.2f}%\n"
            msg += f"  Entry: ${trade.get('entry_price', 0):.6f} -> Exit: ${trade.get('exit_price', 0):.6f}\n\n"
        await self.send_message(msg, chat_id)

    async def _cmd_pause(self, chat_id: str):
        if not self.trader:
            await self.send_message("Trader not available.", chat_id)
            return
        self.trader._paused = True
        await self.send_message("⏸ Bot paused. Use /resume to continue.", chat_id)

    async def _cmd_resume(self, chat_id: str):
        if not self.trader:
            await self.send_message("Trader not available.", chat_id)
            return
        self.trader._paused = False
        await self.send_message("▶️ Bot resumed.", chat_id)

    async def _cmd_help(self, chat_id: str):
        msg = "🤖 <b>Trading Bot Commands</b>\n\n"
        msg += "/status - Bot status overview\n"
        msg += "/positions - Active positions\n"
        msg += "/metrics - Performance metrics\n"
        msg += "/history - Recent trade history\n"
        msg += "/pause - Pause trading\n"
        msg += "/resume - Resume trading\n"
        msg += "/help - Show this help"
        await self.send_message(msg, chat_id)

    async def start_polling(self):
        if not self.enabled:
            return
        await self.initialize()
        await self.send_message("🤖 Trading bot started! Use /help for commands.")
        logger.info("Telegram bot polling started.")

        while True:
            try:
                updates = await self._get_updates()
                for update in updates:
                    self._last_update_id = update.get("update_id", self._last_update_id)
                    message = update.get("message", {})
                    text = message.get("text", "")
                    chat_id = str(message.get("chat", {}).get("id", ""))

                    if text.startswith("/") and chat_id:
                        if chat_id == self.chat_id:
                            await self._handle_command(text, chat_id)
                        else:
                            await self.send_message("Unauthorized.", chat_id)

                await asyncio.sleep(2)
            except asyncio.CancelledError:
                logger.info("Telegram polling cancelled.")
                break
            except Exception as e:
                logger.error(f"Telegram polling error: {e}")
                await asyncio.sleep(10)
