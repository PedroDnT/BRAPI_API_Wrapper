import unittest
from brapi_wrapper import fetch_quote

class TestBrapiWrapper(unittest.TestCase):
    def test_fetch_quote_single_ticker(self):
        result = fetch_quote(tickers=["AAPL"])
        self.assertIsNotNone(result)
        self.assertIn("AAPL", result)

    def test_fetch_quote_multiple_tickers(self):
        result = fetch_quote(tickers=["AAPL", "GOOGL"])
        self.assertIsNotNone(result)
        self.assertIn("AAPL", result)
        self.assertIn("GOOGL", result)

    def test_fetch_quote_invalid_ticker(self):
        result = fetch_quote(tickers=["INVALID"])
        self.assertIsNotNone(result)
        self.assertNotIn("INVALID", result)

if __name__ == '__main__':
    unittest.main() 