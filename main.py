import asyncio
import uvicorn
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from trading_bot.trader import SolanaTrader
from trading_bot.scanner import TokenScanner
from trading_bot.telegram_bot import TelegramBot

trader = SolanaTrader()
scanner = TokenScanner()
telegram_bot = TelegramBot(trader=trader, scanner=scanner)
trader.telegram = telegram_bot

_background_tasks = []

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    await scanner.initialize()
    scan_task = asyncio.create_task(scanner.start_scanning())
    trade_task = asyncio.create_task(trader.start_trading(scanner))
    _background_tasks.extend([scan_task, trade_task])
    if telegram_bot.enabled:
        tg_task = asyncio.create_task(telegram_bot.start_polling())
        _background_tasks.append(tg_task)
    yield
    # Shutdown
    for task in _background_tasks:
        task.cancel()
    await asyncio.gather(*_background_tasks, return_exceptions=True)
    await scanner.close()
    await trader.close_session()
    await telegram_bot.close()

app = FastAPI(lifespan=lifespan)
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/")
async def get_index():
    return FileResponse("static/index.html")

@app.get("/api/status")
async def get_status():
    active_positions = {}
    for addr, pos in trader.active_positions.items():
        current_price = await trader.get_token_price(addr)
        active_positions[addr] = {
            **pos,
            'current_price': current_price
        }

    scanner_metrics_data = scanner.get_scanner_metrics()

    return {
        "metrics": trader.performance_metrics,
        "active_positions": active_positions,
        "position_history": trader.position_history,
        "scanner_metrics": scanner_metrics_data
    }

@app.get("/api/trades")
async def get_trades(limit: int = 50):
    return trader.db.get_trade_history(limit=limit)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
