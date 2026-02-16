import unittest
from unittest.mock import patch, AsyncMock
from fastapi.testclient import TestClient
from datetime import datetime, timedelta

# main.py is at the project root, so import from main directly
from main import app, scanner, trader


class TestMainAPI(unittest.TestCase):

    def setUp(self):
        self.client = TestClient(app)

    @patch('main.scanner.get_scanner_metrics')
    def test_get_api_status(self, mock_get_scanner_metrics):
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

        original_active_positions = trader.active_positions
        original_performance_metrics = trader.performance_metrics
        original_position_history = trader.position_history

        trader.active_positions = {}
        trader.performance_metrics = {"profit_loss": 100.0, "win_rate": 0.75}
        trader.position_history = [{"symbol": "OLDTOK", "outcome": "profit", "pnl": 50.0}]

        response = self.client.get("/api/status")

        self.assertEqual(response.status_code, 200)

        data = response.json()

        self.assertIn("scanner_metrics", data)
        self.assertEqual(data["scanner_metrics"], mock_scanner_data)
        mock_get_scanner_metrics.assert_called_once()

        self.assertIn("metrics", data)
        self.assertEqual(data["metrics"], trader.performance_metrics)

        self.assertIn("active_positions", data)
        self.assertEqual(data["active_positions"], {})

        self.assertIn("position_history", data)
        self.assertEqual(data["position_history"], trader.position_history)

        trader.active_positions = original_active_positions
        trader.performance_metrics = original_performance_metrics
        trader.position_history = original_position_history

if __name__ == '__main__':
    unittest.main()
