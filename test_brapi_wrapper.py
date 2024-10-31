import unittest
import pandas as pd
import numpy as np
from brapi_wrapper import (
    fetch_quote,
    fetch_balance_sheet_history,
    fetch_income_statement_history,
    fetch_income_statement_history_quarterly,
    fetch_balance_sheet_history_quarterly,
    fetch_default_key_statistics,
    fetch_financial_data,
    fetch_summary_profile,
    fetch_quote_list,
    fetch_available_tickers
)

class TestBrapiWrapper(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Set up test fixtures before running tests"""
        cls.test_ticker = 'PETR4.SA'
        cls.test_tickers = ['PETR4.SA', 'VALE3.SA']
        
    def test_fetch_quote(self):
        """Test fetching quote data"""
        # Test single ticker
        result = fetch_quote(self.test_ticker)
        self.assertIsNotNone(result)
        self.assertIsInstance(result, pd.DataFrame)
        self.assertTrue('close' in result.columns)
        
        # Test multiple tickers
        results = fetch_quote(self.test_tickers)
        self.assertIsNotNone(results)
        self.assertIsInstance(results, dict)
        self.assertEqual(len(results), len(self.test_tickers))
        
    def test_fetch_balance_sheet_history(self):
        """Test fetching balance sheet history"""
        result = fetch_balance_sheet_history(self.test_ticker)
        self.assertIsNotNone(result)
        self.assertIsInstance(result, dict)
        self.assertTrue(self.test_ticker in result)
        
        df = result[self.test_ticker]
        self.assertIsInstance(df, pd.DataFrame)
        self.assertTrue(len(df) > 0)
        
    def test_fetch_income_statement_history(self):
        """Test fetching income statement history"""
        result = fetch_income_statement_history(self.test_ticker)
        self.assertIsNotNone(result)
        self.assertIsInstance(result, dict)
        self.assertTrue(self.test_ticker in result)
        
        df = result[self.test_ticker]
        self.assertIsInstance(df, pd.DataFrame)
        self.assertTrue(len(df) > 0)
        
    def test_fetch_income_statement_history_quarterly(self):
        """Test fetching quarterly income statement history"""
        result = fetch_income_statement_history_quarterly(self.test_ticker)
        self.assertIsNotNone(result)
        self.assertIsInstance(result, dict)
        self.assertTrue(self.test_ticker in result)
        
        df = result[self.test_ticker]
        self.assertIsInstance(df, pd.DataFrame)
        self.assertTrue(len(df) > 0)
        
    def test_fetch_balance_sheet_history_quarterly(self):
        """Test fetching quarterly balance sheet history"""
        result = fetch_balance_sheet_history_quarterly(self.test_ticker)
        self.assertIsNotNone(result)
        self.assertIsInstance(result, dict)
        self.assertTrue(self.test_ticker in result)
        
        df = result[self.test_ticker]
        self.assertIsInstance(df, pd.DataFrame)
        self.assertTrue(len(df) > 0)
        
    def test_fetch_default_key_statistics(self):
        """Test fetching key statistics"""
        result = fetch_default_key_statistics(self.test_ticker)
        self.assertIsNotNone(result)
        self.assertIsInstance(result, pd.DataFrame)
        self.assertTrue(self.test_ticker in result.index)
        
    def test_fetch_financial_data(self):
        """Test fetching financial data"""
        result = fetch_financial_data(self.test_ticker)
        self.assertIsNotNone(result)
        self.assertIsInstance(result, pd.DataFrame)
        self.assertTrue(self.test_ticker in result.index)
        
    def test_fetch_summary_profile(self):
        """Test fetching company profile"""
        result = fetch_summary_profile(self.test_ticker)
        self.assertIsNotNone(result)
        self.assertIsInstance(result, pd.DataFrame)
        self.assertTrue(self.test_ticker in result.index)
        
    def test_fetch_quote_list(self):
        """Test fetching quote list"""
        result = fetch_quote_list()
        self.assertIsNotNone(result)
        self.assertIsInstance(result, dict)
        self.assertTrue('stocks' in result)
        self.assertIsInstance(result['stocks'], pd.DataFrame)
        
    def test_fetch_available_tickers(self):
        """Test fetching available tickers"""
        result = fetch_available_tickers()
        self.assertIsNotNone(result)
        self.assertIsInstance(result, pd.DataFrame)
        self.assertTrue(len(result) > 0)
        
    def test_error_handling(self):
        """Test error handling with invalid inputs"""
        # Test with invalid ticker
        result = fetch_quote('INVALID')
        self.assertIsNone(result)
        
        # Test with invalid type
        with self.assertRaises(ValueError):
            fetch_quote(123)
            
    def test_data_validation(self):
        """Test data validation and cleaning"""
        result = fetch_quote(self.test_ticker)
        self.assertIsNotNone(result)
        
        # Check for NaN values
        self.assertFalse(result['close'].isnull().all())
        
        # Check date index
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(result.index))

if __name__ == '__main__':
    unittest.main(verbosity=2) 