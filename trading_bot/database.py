import sqlite3
import logging
import os
from datetime import datetime
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "trades.db")


class TradeDatabase:
    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        self.conn: Optional[sqlite3.Connection] = None
        self._initialize()

    def _initialize(self):
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._create_tables()
        logger.info(f"Trade database initialized at {self.db_path}")

    def _create_tables(self):
        cursor = self.conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                token_address TEXT NOT NULL,
                symbol TEXT DEFAULT 'N/A',
                action TEXT NOT NULL,
                amount REAL NOT NULL,
                price REAL,
                entry_price REAL,
                exit_price REAL,
                profit_loss_pct REAL,
                stop_loss REAL,
                take_profit REAL,
                dry_run INTEGER DEFAULT 1,
                timestamp TEXT NOT NULL
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS positions (
                token_address TEXT PRIMARY KEY,
                symbol TEXT DEFAULT 'N/A',
                entry_price REAL NOT NULL,
                amount REAL NOT NULL,
                stop_loss REAL,
                take_profit REAL,
                highest_price_since_entry REAL,
                entry_time TEXT NOT NULL,
                dry_run INTEGER DEFAULT 1
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS metrics (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                scans INTEGER DEFAULT 0,
                potential_trades INTEGER DEFAULT 0,
                executed_trades INTEGER DEFAULT 0,
                successful_trades INTEGER DEFAULT 0,
                failed_trades INTEGER DEFAULT 0,
                total_profit_loss REAL DEFAULT 0,
                win_rate REAL DEFAULT 0,
                updated_at TEXT
            )
        """)
        cursor.execute("""
            INSERT OR IGNORE INTO metrics (id, scans, potential_trades, executed_trades,
                successful_trades, failed_trades, total_profit_loss, win_rate, updated_at)
            VALUES (1, 0, 0, 0, 0, 0, 0, 0, ?)
        """, (datetime.now().isoformat(),))
        self.conn.commit()

    def record_trade(self, token_address: str, symbol: str, action: str, amount: float,
                     price: float = None, entry_price: float = None, exit_price: float = None,
                     profit_loss_pct: float = None, stop_loss: float = None,
                     take_profit: float = None, dry_run: bool = True):
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT INTO trades (token_address, symbol, action, amount, price, entry_price,
                exit_price, profit_loss_pct, stop_loss, take_profit, dry_run, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (token_address, symbol, action, amount, price, entry_price, exit_price,
              profit_loss_pct, stop_loss, take_profit, 1 if dry_run else 0,
              datetime.now().isoformat()))
        self.conn.commit()

    def save_position(self, token_address: str, position: dict, dry_run: bool = True):
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO positions (token_address, symbol, entry_price, amount,
                stop_loss, take_profit, highest_price_since_entry, entry_time, dry_run)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (token_address, position.get('symbol', 'N/A'), position['entry_price'],
              position['amount'], position.get('stop_loss'), position.get('take_profit'),
              position.get('highest_price_since_entry'), position.get('entry_time'),
              1 if dry_run else 0))
        self.conn.commit()

    def remove_position(self, token_address: str):
        cursor = self.conn.cursor()
        cursor.execute("DELETE FROM positions WHERE token_address = ?", (token_address,))
        self.conn.commit()

    def load_positions(self) -> Dict[str, dict]:
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM positions")
        rows = cursor.fetchall()
        positions = {}
        for row in rows:
            positions[row['token_address']] = {
                'symbol': row['symbol'],
                'entry_price': row['entry_price'],
                'amount': row['amount'],
                'stop_loss': row['stop_loss'],
                'take_profit': row['take_profit'],
                'highest_price_since_entry': row['highest_price_since_entry'],
                'entry_time': row['entry_time']
            }
        return positions

    def save_metrics(self, metrics: dict):
        cursor = self.conn.cursor()
        cursor.execute("""
            UPDATE metrics SET scans = ?, potential_trades = ?, executed_trades = ?,
                successful_trades = ?, failed_trades = ?, total_profit_loss = ?,
                win_rate = ?, updated_at = ?
            WHERE id = 1
        """, (metrics.get('scans', 0), metrics.get('potential_trades', 0),
              metrics.get('executed_trades', 0), metrics.get('successful_trades', 0),
              metrics.get('failed_trades', 0), metrics.get('total_profit_loss', 0),
              metrics.get('win_rate', 0), datetime.now().isoformat()))
        self.conn.commit()

    def load_metrics(self) -> dict:
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM metrics WHERE id = 1")
        row = cursor.fetchone()
        if row:
            return {
                'scans': row['scans'],
                'potential_trades': row['potential_trades'],
                'executed_trades': row['executed_trades'],
                'successful_trades': row['successful_trades'],
                'failed_trades': row['failed_trades'],
                'total_profit_loss': row['total_profit_loss'],
                'win_rate': row['win_rate']
            }
        return {
            'scans': 0, 'potential_trades': 0, 'executed_trades': 0,
            'successful_trades': 0, 'failed_trades': 0, 'total_profit_loss': 0, 'win_rate': 0
        }

    def get_trade_history(self, limit: int = 50) -> List[dict]:
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT * FROM trades WHERE action = 'SELL'
            ORDER BY timestamp DESC LIMIT ?
        """, (limit,))
        rows = cursor.fetchall()
        return [dict(row) for row in rows]

    def close(self):
        if self.conn:
            self.conn.close()
            logger.info("Trade database connection closed.")
