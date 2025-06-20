import unittest
from unittest.mock import patch, AsyncMock # AsyncMock might be needed if we don't simplify trader calls
from fastapi.testclient import TestClient
from datetime import datetime

# Import app and global objects from main.py
# Ensure that main.py can be imported in a test environment
# (e.g., it doesn't start a web server immediately upon import if not __main__)
from trading_bot.main import app, scanner, trader # Assuming scanner and trader are global instances in main

class TestMainAPI(unittest.TestCase):

    def setUp(self):
        self.client = TestClient(app)
        # Reset or set default states for global trader/scanner objects if necessary,
        # though patching their methods/attributes is often cleaner for API tests.

    @patch('trading_bot.main.scanner.get_scanner_metrics')
    # If trader.get_token_price was complex or made external calls, we'd mock it:
    # @patch('trading_bot.main.trader.get_token_price', new_callable=AsyncMock)
    def test_get_api_status(self, mock_get_scanner_metrics): #, mock_get_token_price
        # 1. Prepare mock data for scanner.get_scanner_metrics()
        mock_scanner_data = {
            "unique_tokens_scanned_today": 15,
            "total_unique_tokens_scanned_all_time": 150,
            "potential_tokens_count": 7,
            "potential_tokens_recent": [
                {"symbol": "TOK1", "pair_address_short": "p1..s1", "price_usd": "1.2345", "discovered_at": datetime.now().isoformat()},
                {"symbol": "TOK2", "pair_address_short": "p2..s2", "price_usd": "0.005", "discovered_at": (datetime.now() - timedelta(hours=1)).isoformat()},
            ],
            "rugcheck_jwt_status": "Loaded",
            "last_daily_reset_at": (datetime.now() - timedelta(hours=5)).isoformat()
        }
        mock_get_scanner_metrics.return_value = mock_scanner_data

        # 2. Prepare mock data/state for trader object properties accessed by the endpoint
        # To avoid trader.get_token_price calls, ensure active_positions is empty for this test
        original_active_positions = trader.active_positions
        original_performance_metrics = trader.performance_metrics
        original_position_history = trader.position_history

        trader.active_positions = {} # No active positions, so get_token_price won't be called
        trader.performance_metrics = {"profit_loss": 100.0, "win_rate": 0.75}
        trader.position_history = [{"symbol": "OLDTOK", "outcome": "profit", "pnl": 50.0}]

        # If we had active_positions that trigger get_token_price:
        # mock_get_token_price.return_value = 1.23 # Example mock price

        # 3. Make API Call
        response = self.client.get("/api/status")

        # 4. Assert Status Code
        self.assertEqual(response.status_code, 200)

        # 5. Assert Response Content
        data = response.json()

        self.assertIn("scanner_metrics", data)
        self.assertEqual(data["scanner_metrics"], mock_scanner_data)
        mock_get_scanner_metrics.assert_called_once() # Verify the mock was called

        self.assertIn("trader_metrics", data)
        self.assertEqual(data["trader_metrics"], trader.performance_metrics)

        self.assertIn("active_positions", data)
        self.assertEqual(data["active_positions"], {}) # As we set it to empty

        self.assertIn("position_history", data)
        self.assertEqual(data["position_history"], trader.position_history)

        # Restore original trader attributes if they were modified directly on the global instance
        trader.active_positions = original_active_positions
        trader.performance_metrics = original_performance_metrics
        trader.position_history = original_position_history

if __name__ == '__main__':
    unittest.main()
