import requests
import pandas as pd
from datetime import datetime
import os
from dotenv import load_dotenv
import numpy as np
from typing import List, Dict, Optional, Union, Any
import logging

load_dotenv()

BASE_URL = "https://brapi.dev/"
API_KEY = os.getenv("BRAPI_TOKEN")

# Debug prints after API_KEY is defined
logger = logging.getLogger(__name__)
logger.info(f"API_KEY loaded: {'yes' if API_KEY else 'no'}")
logger.info(f"API_KEY length: {len(str(API_KEY)) if API_KEY else 0}")

def _convert_to_float64(df):
    """Convert numeric columns to float64 (double precision)"""
    for col in df.columns:
        try:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('float64')
        except:
            continue
    return df

def make_request(endpoint, params=None):
    """Make a request to the Brapi API
    
    Args:
        endpoint (str): API endpoint path
        params (dict, optional): Query parameters. Defaults to None.
        
    Returns:
        dict/list: API response data if successful, None if failed
    """
    if params is None:
        params = {}
    
    # Add token to params if it exists
    if API_KEY:
        params['token'] = API_KEY
    
    try:
        url = f"{BASE_URL}{endpoint}"
        logger.debug(f"Making request to: {url}")
        logger.debug(f"With params: {params}")
        
        response = requests.get(url, params=params)
        logger.debug(f"Response status code: {response.status_code}")
        
        # Handle common error status codes based on documentation
        if response.status_code == 400:
            logger.warning("Bad Request: The request was malformed or invalid")
            return None
            
        if response.status_code == 401:
            logger.warning("Unauthorized: Invalid or missing authentication token")
            return None
            
        if response.status_code == 402:
            logger.warning("Payment Required: API request limit reached")
            return None
            
        if response.status_code == 404:
            logger.warning("Not Found: Requested resource not found")
            return None
            
        if response.status_code == 417:
            logger.warning("Expectation Failed: Invalid query parameters")
            return None
            
        if response.status_code != 200:
            logger.error(f"Error response: {response.text}")
            return None
            
        json_response = response.json()
        logger.debug(f"Response content: {str(json_response)[:500]}...")
        
        # Check for error field in response
        if isinstance(json_response, dict) and json_response.get('error'):
            logger.error(f"API Error: {json_response.get('message', 'Unknown error')}")
            return None
            
        # Handle different response formats based on endpoint
        if isinstance(json_response, dict):
            # Quote endpoint
            if 'results' in json_response:
                return json_response['results']
            
            # List endpoint    
            if 'stocks' in json_response:
                return json_response['stocks']
                
            # Currency endpoint
            if 'currency' in json_response:
                return json_response['currency']
                
            # Inflation endpoint
            if 'inflation' in json_response:
                return json_response['inflation']
                
            # Prime rate endpoint    
            if 'prime-rate' in json_response:
                return json_response['prime-rate']
                
            # Crypto endpoint
            if 'coins' in json_response:
                return json_response['coins']
                
            # Return full response if no specific field found
            return json_response
            
        return json_response
            
    except requests.exceptions.RequestException as e:
        logger.error(f"Request error for {endpoint}: {str(e)}")
        return None
    except ValueError as e:
        logger.error(f"JSON decode error for {endpoint}: {str(e)}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error for {endpoint}: {str(e)}")
        return None

def fetch_quote(tickers, range='1d', interval='1d', fundamental=False, dividends=False, modules=None):
    """Fetch quote data with expanded options"""
    try:
        # Handle single ticker or list of tickers
        if isinstance(tickers, str):
            tickers = [tickers]
        elif not isinstance(tickers, (list, tuple)):
            raise ValueError(f"Expected string or list of tickers, got {type(tickers)}")
            
        results = {}
        for ticker in tickers:
            if not isinstance(ticker, str):
                logger.warning(f"Skipping invalid ticker: {ticker}")
                continue
                
            ticker_sa = ticker.replace('.SA', '') + '.SA'
            
            params = {
                'range': range,
                'interval': interval,
                'fundamental': str(fundamental).lower(),
                'dividends': str(dividends).lower()
            }
            
            if modules:
                params['modules'] = modules
            
            response = make_request(f'api/quote/{ticker_sa}', params)
            if not response:
                continue
                
            # Extract historical price data
            historical_data = None
            if isinstance(response, dict):
                historical_data = response.get('historicalDataPrice', [])
            elif isinstance(response, list) and response:
                historical_data = response[0].get('historicalDataPrice', [])
                
            if historical_data:
                # Create DataFrame for this ticker
                df = pd.DataFrame(historical_data)
                
                # Convert date based on interval
                intraday_intervals = ['1m', '2m', '5m', '15m', '30m', '60m', '90m', '1h']
                if interval in intraday_intervals:
                    df['date'] = pd.to_datetime(df['date'], unit='s')
                else:
                    df['date'] = pd.to_datetime(df['date'], unit='s')
                    
                df.set_index('date', inplace=True)
                
                # Add fundamental data if requested
                if fundamental and isinstance(response, dict):
                    fund_data = response.get('fundamentals', {})
                    for key, value in fund_data.items():
                        df[f'fundamental_{key}'] = value
                        
                # Add dividend data if requested    
                if dividends and isinstance(response, dict):
                    div_data = response.get('dividendsData', {})
                    if div_data:
                        div_df = pd.DataFrame(div_data)
                        div_df['date'] = pd.to_datetime(div_df['date'])
                        div_df.set_index('date', inplace=True)
                        df = df.join(div_df, how='left')
                        
                results[ticker] = df
                
        return results if len(tickers) > 1 else results[tickers[0]] if results else None
        
    except Exception as e:
        logger.error(f"Error fetching quote data: {str(e)}")
        return None

def fetch_quote_list(search=None, sortBy=None, sortOrder='desc', limit=None, sector=None):
    """Fetch list of quotes with filtering and sorting
    
    Args:
        search (str, optional): Search term for filtering
        sortBy (str, optional): Sort field (name,close,change,volume,market_cap)
        sortOrder (str, optional): Sort order ('asc' or 'desc')
        limit (int, optional): Number of results to return
        sector (str, optional): Filter by sector
        
    Returns:
        dict: Dictionary containing stocks DataFrame
    """
    try:
        params = {}
        if search:
            params['search'] = search
        if sortBy:
            params['sortBy'] = sortBy
        if sortOrder:
            params['sortOrder'] = sortOrder
        if limit:
            params['limit'] = limit
        if sector:
            params['sector'] = sector
            
        response = make_request('api/quote/list', params)
        if response:
            return {'stocks': pd.DataFrame(response)}
        return {'stocks': pd.DataFrame()}
    except Exception as e:
        logger.error(f"Error fetching quote list: {str(e)}")
        return {'stocks': pd.DataFrame()}

def fetch_currency(currencies, token=None):
    """Fetch currency exchange rates"""
    try:
        params = {'currency': currencies}
        if token:
            params['token'] = token
            
        response = make_request('/api/v2/currency', params)
        if response and isinstance(response, list):
            df = pd.DataFrame(response)
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date']).strftime('%Y-%m-%d')
                df.set_index('date', inplace=True)
            return df
        return None
    except Exception as e:
        logger.error(f"Error fetching currency data: {str(e)}")
        return None

def fetch_crypto(coins, currency='BRL'):
    """Fetch cryptocurrency prices"""
    try:
        params = {
            'coin': coins,
            'currency': currency
        }
        
        response = make_request('/api/v2/crypto', params)
        if response and isinstance(response, list):
            df = pd.DataFrame(response)
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date']).strftime('%Y-%m-%d')
                df.set_index('date', inplace=True)
            return df
        return None
    except Exception as e:
        logger.error(f"Error fetching crypto data: {str(e)}")
        return None

def fetch_available_tickers(search=None):
    """Fetch available tickers from the Brapi API
    
    Args:
        search (str, optional): Search term to filter tickers
        
    Returns:
        pd.DataFrame: DataFrame with stock information including columns:
            - stock: ticker symbol
            - name: company name
            - close: last closing price
            - change: price change
            - volume: trading volume
            - market_cap: market capitalization
    """
    try:
        params = {}
        if search:
            params['search'] = search
            
        response = make_request('/api/quote/list', params)
        
        if response and isinstance(response, list):
            # Create DataFrame from response
            df = pd.DataFrame(response)
            
            # Rename columns for clarity if needed
            column_mapping = {
                'stock': 'stock',
                'name': 'name',
                'close': 'close',
                'change': 'change',
                'volume': 'volume',
                'market_cap': 'market_cap'
            }
            
            # Select and rename columns if they exist
            available_columns = [col for col in column_mapping.keys() if col in df.columns]
            df = df[available_columns].rename(columns=column_mapping)
            
            # Convert numeric columns
            numeric_columns = ['close', 'change', 'volume', 'market_cap']
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Sort by stock symbol
            df.sort_values('stock', inplace=True)
            
            # Reset index
            df.reset_index(drop=True, inplace=True)
            
            return df
            
        return pd.DataFrame()
        
    except Exception as e:
        logger.error(f"Error fetching available tickers: {str(e)}")
        return pd.DataFrame()

def fetch_balance_sheet_history(tickers):
    """Fetch balance sheet history for multiple tickers"""
    try:
        if isinstance(tickers, str):
            tickers = [tickers]
        elif not isinstance(tickers, (list, tuple)):
            raise ValueError(f"Expected string or list of tickers, got {type(tickers)}")
            
        results = {}
        for ticker in tickers:
            if not isinstance(ticker, str):
                logger.warning(f"Skipping invalid ticker: {ticker}")
                continue
                
            ticker_sa = ticker.replace('.SA', '') + '.SA'
            
            params = {
                'fundamental': 'true',
                'modules': 'balanceSheetHistory'
            }
            
            response = make_request(f'api/quote/{ticker_sa}', params)
            if not response:
                continue
                
            if isinstance(response, list):
                response = response[0] if response else {}
            
            if isinstance(response, dict):
                balance_sheet_data = (response.get('balanceSheetHistory', {})
                                    .get('balanceSheetStatements', []))
                
                if balance_sheet_data:
                    data_dict = {}
                    
                    for statement in balance_sheet_data:
                        if 'endDate' in statement:
                            date = pd.to_datetime(statement['endDate']).strftime('%Y-%m-%d')
                            
                            for key, value in statement.items():
                                if key != 'endDate':
                                    if key not in data_dict:
                                        data_dict[key] = {}
                                    # Convert to thousands for better readability
                                    data_dict[key][date] = value / 1000 if isinstance(value, (int, float)) else value
                    
                    df = pd.DataFrame(data_dict).transpose()
                    df = df.apply(pd.to_numeric, errors='coerce')
                    df = df.sort_index(axis=1)
                    df.index = [idx.replace('_', ' ').title() for idx in df.index]
                    
                    # Format numbers to avoid scientific notation
                    pd.options.display.float_format = '{:,.0f}'.format
                    
                    results[ticker] = df
                    
        return results if results else None
        
    except Exception as e:
        logger.error(f"Error fetching balance sheet: {str(e)}")
        return None

def fetch_income_statement_history(tickers):
    """Fetch annual income statement history for multiple tickers"""
    try:
        if isinstance(tickers, str):
            tickers = [tickers]
        elif not isinstance(tickers, (list, tuple)):
            raise ValueError(f"Expected string or list of tickers, got {type(tickers)}")
            
        results = {}
        for ticker in tickers:
            if not isinstance(ticker, str):
                logger.warning(f"Skipping invalid ticker: {ticker}")
                continue
                
            ticker_sa = ticker.replace('.SA', '') + '.SA'
            
            params = {
                'fundamental': 'true',
                'modules': 'incomeStatementHistory'
            }
            
            response = make_request(f'api/quote/{ticker_sa}', params)
            if not response:
                continue
                
            if isinstance(response, list):
                response = response[0] if response else {}
                
            if isinstance(response, dict):
                income_stmt = (response.get('incomeStatementHistory', {})
                             .get('incomeStatementHistory', []))
                
                if income_stmt:
                    data_dict = {}
                    
                    for statement in income_stmt:
                        if 'endDate' in statement:
                            date = pd.to_datetime(statement['endDate']).strftime('%Y-%m-%d')
                            
                            for key, value in statement.items():
                                if key != 'endDate':
                                    if key not in data_dict:
                                        data_dict[key] = {}
                                    data_dict[key][date] = value
                    
                    df = pd.DataFrame(data_dict).transpose()
                    df = df.apply(pd.to_numeric, errors='coerce')
                    df = df.sort_index(axis=1)
                    df.index = [idx.replace('_', ' ').title() for idx in df.index]
                    
                    results[ticker] = df
                    
        return results if results else None
        
    except Exception as e:
        logger.error(f"Error fetching income statement: {str(e)}")
        return None

def fetch_income_statement_history_quarterly(tickers):
    """Fetch quarterly income statement history for multiple tickers"""
    try:
        if isinstance(tickers, str):
            tickers = [tickers]
        elif not isinstance(tickers, (list, tuple)):
            raise ValueError(f"Expected string or list of tickers, got {type(tickers)}")
            
        results = {}
        for ticker in tickers:
            if not isinstance(ticker, str):
                logger.warning(f"Skipping invalid ticker: {ticker}")
                continue
                
            ticker_sa = ticker.replace('.SA', '') + '.SA'
            
            params = {
                'fundamental': 'true',
                'modules': 'incomeStatementHistoryQuarterly'
            }
            
            response = make_request(f'api/quote/{ticker_sa}', params)
            if not response:
                continue
                
            if isinstance(response, list):
                response = response[0] if response else {}
                
            if isinstance(response, dict):
                income_stmt = (response.get('incomeStatementHistoryQuarterly', {})
                             .get('incomeStatementHistory', []))
                
                if income_stmt:
                    data_dict = {}
                    
                    for statement in income_stmt:
                        if 'endDate' in statement:
                            date = pd.to_datetime(statement['endDate']).strftime('%Y-%m-%d')
                            
                            for key, value in statement.items():
                                if key != 'endDate':
                                    if key not in data_dict:
                                        data_dict[key] = {}
                                    data_dict[key][date] = value
                    
                    df = pd.DataFrame(data_dict).transpose()
                    df = df.apply(pd.to_numeric, errors='coerce')
                    df = df.sort_index(axis=1)
                    df.index = [idx.replace('_', ' ').title() for idx in df.index]
                    
                    results[ticker] = df
                    
        return results if results else None
        
    except Exception as e:
        logger.error(f"Error fetching quarterly income statement: {str(e)}")
        return None

def fetch_balance_sheet_history_quarterly(tickers):
    """Fetch quarterly balance sheet history for multiple tickers
    
    Args:
        tickers (str or list): Single ticker or list of ticker symbols
        
    Returns:
        dict: Dictionary with tickers as keys and DataFrames as values (values in thousands)
    """
    try:
        if isinstance(tickers, str):
            tickers = [tickers]
        elif not isinstance(tickers, (list, tuple)):
            raise ValueError(f"Expected string or list of tickers, got {type(tickers)}")
            
        results = {}
        for ticker in tickers:
            if not isinstance(ticker, str):
                logger.warning(f"Skipping invalid ticker: {ticker}")
                continue
                
            ticker_sa = ticker.replace('.SA', '') + '.SA'
            
            params = {
                'fundamental': 'true',
                'modules': 'balanceSheetHistoryQuarterly'
            }
            
            response = make_request(f'api/quote/{ticker_sa}', params)
            if not response:
                continue
                
            if isinstance(response, list):
                response = response[0] if response else {}
            
            if isinstance(response, dict):
                balance_sheet_data = (response.get('balanceSheetHistoryQuarterly', {})
                                    .get('balanceSheetStatements', []))
                
                if balance_sheet_data:
                    data_dict = {}
                    
                    for statement in balance_sheet_data:
                        if 'endDate' in statement:
                            date = pd.to_datetime(statement['endDate']).strftime('%Y-%m-%d')
                            
                            for key, value in statement.items():
                                if key != 'endDate':
                                    if key not in data_dict:
                                        data_dict[key] = {}
                                    # Convert to thousands for better readability
                                    data_dict[key][date] = value / 1000 if isinstance(value, (int, float)) else value
                    
                    df = pd.DataFrame(data_dict).transpose()
                    df = df.apply(pd.to_numeric, errors='coerce')
                    df = df.sort_index(axis=1)
                    df.index = [idx.replace('_', ' ').title() for idx in df.index]
                    
                    # Format numbers to avoid scientific notation
                    pd.options.display.float_format = '{:,.0f}'.format
                    
                    results[ticker] = df
                    
        return results if results else None
        
    except Exception as e:
        logger.error(f"Error fetching quarterly balance sheet: {str(e)}")
        return None

def fetch_default_key_statistics(tickers):
    """Fetch key statistics for multiple tickers"""
    try:
        if isinstance(tickers, str):
            tickers = [tickers]
        elif not isinstance(tickers, (list, tuple)):
            raise ValueError(f"Expected string or list of tickers, got {type(tickers)}")
            
        all_stats = []
        for ticker in tickers:
            if not isinstance(ticker, str):
                logger.warning(f"Skipping invalid ticker: {ticker}")
                continue
                
            ticker_sa = ticker.replace('.SA', '') + '.SA'
            
            params = {
                'fundamental': 'true',
                'modules': 'defaultKeyStatistics'
            }
            
            response = make_request(f'api/quote/{ticker_sa}', params)
            if not response:
                continue
                
            if isinstance(response, list):
                response = response[0] if response else {}
                
            if isinstance(response, dict):
                stats = response.get('defaultKeyStatistics', {})
                if stats:
                    stats['ticker'] = ticker  # Add ticker column
                    all_stats.append(stats)
        
        if all_stats:
            df = pd.DataFrame(all_stats)
            df.set_index('ticker', inplace=True)
            
            # Convert numeric columns
            for col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                
            return df
            
        return None
        
    except Exception as e:
        logger.error(f"Error fetching key statistics: {str(e)}")
        return None

def fetch_financial_data(tickers):
    """Fetch financial data for multiple tickers"""
    try:
        if isinstance(tickers, str):
            tickers = [tickers]
        elif not isinstance(tickers, (list, tuple)):
            raise ValueError(f"Expected string or list of tickers, got {type(tickers)}")
            
        all_data = []
        for ticker in tickers:
            if not isinstance(ticker, str):
                logger.warning(f"Skipping invalid ticker: {ticker}")
                continue
                
            ticker_sa = ticker.replace('.SA', '') + '.SA'
            
            params = {
                'fundamental': 'true',
                'modules': 'financialData'
            }
            
            response = make_request(f'api/quote/{ticker_sa}', params)
            if not response:
                continue
                
            if isinstance(response, list):
                response = response[0] if response else {}
                
            if isinstance(response, dict):
                fin_data = response.get('financialData', {})
                if fin_data:
                    fin_data['ticker'] = ticker
                    all_data.append(fin_data)
        
        if all_data:
            df = pd.DataFrame(all_data)
            df.set_index('ticker', inplace=True)
            
            # Convert numeric columns preserving decimals
            for col in df.columns:
                try:
                    df[col] = pd.to_numeric(df[col], errors='coerce', downcast=None)
                except:
                    continue
                    
            # Avoid scientific notation
            pd.set_option('display.float_format', lambda x: '%.4f' % x if isinstance(x, float) else str(x))
                
            return df
            
        return None
        
    except Exception as e:
        logger.error(f"Error fetching financial data: {str(e)}")
        return None

def fetch_summary_profile(tickers):
    """Fetch company profile information for multiple tickers"""
    try:
        if isinstance(tickers, str):
            tickers = [tickers]
        elif not isinstance(tickers, (list, tuple)):
            raise ValueError(f"Expected string or list of tickers, got {type(tickers)}")
            
        all_profiles = []
        for ticker in tickers:
            if not isinstance(ticker, str):
                logger.warning(f"Skipping invalid ticker: {ticker}")
                continue
                
            ticker_sa = ticker.replace('.SA', '') + '.SA'
            
            params = {
                'fundamental': 'true',
                'modules': 'summaryProfile'
            }
            
            response = make_request(f'api/quote/{ticker_sa}', params)
            if not response:
                continue
                
            if isinstance(response, list):
                response = response[0] if response else {}
                
            if isinstance(response, dict):
                profile = response.get('summaryProfile', {})
                if profile:
                    profile['ticker'] = ticker  # Add ticker column
                    all_profiles.append(profile)
        
        if all_profiles:
            df = pd.DataFrame(all_profiles)
            df.set_index('ticker', inplace=True)
            return df
            
        return None
        
    except Exception as e:
        logger.error(f"Error fetching company profiles: {str(e)}")
        return None

def fetch_inflation(start=None, end=None):
    """Fetch Brazilian inflation data
    
    Args:
        start (str, optional): Start date in 'YYYY-MM-DD' format. Defaults to 3 years ago.
        end (str, optional): End date in 'YYYY-MM-DD' format. Defaults to yesterday.
    
    Returns:
        pd.DataFrame: DataFrame with date index and inflation values
    """
    try:
        # Set default dates if not provided
        if start is None:
            start = (datetime.now() - pd.DateOffset(years=3)).strftime('%d/%m/%Y')
        else:
            start = pd.to_datetime(start).strftime('%d/%m/%Y')
            
        if end is None:
            end = (datetime.now() - pd.DateOffset(days=1)).strftime('%d/%m/%Y')
        else:
            end = pd.to_datetime(end).strftime('%d/%m/%Y')
        
        params = { 
            'country': 'brazil',
            'start': start,
            'end': end,
            'sortBy': 'date',
            'sortOrder': 'desc' 
        }
        
        response = make_request('/api/v2/inflation', params)
        
        # Check if response contains inflation data
        if isinstance(response, list) or (isinstance(response, dict) and 'inflation' in response):
            # Extract the data array
            data = response if isinstance(response, list) else response.get('inflation', [])
            
            if data:
                # Create DataFrame
                df = pd.DataFrame(data)
                
                # Convert date to datetime
                df['date'] = pd.to_datetime(df['date'], format='%d/%m/%Y')
                
                # Convert value to numeric, removing any % signs if present
                df['value'] = df['value'].replace({'%': ''}, regex=True)
                df['value'] = pd.to_numeric(df['value'], errors='coerce')
                
                # Set date as index
                df.set_index('date', inplace=True)
                
                # Sort index
                df.sort_index(inplace=True)
                
                # Drop any duplicate indices, keeping the last value
                df = df[~df.index.duplicated(keep='last')]
                
                # Drop epochDate column if it exists
                if 'epochDate' in df.columns:
                    df.drop('epochDate', axis=1, inplace=True)
                
                return df
                
        return None
        
    except Exception as e:
        logger.error(f"Error fetching inflation data: {str(e)}")
        return None

def fetch_prime_rate(start=None, end=None):
    """Fetch Brazilian prime rate (SELIC) data
    
    Args:
        start (str, optional): Start date in 'YYYY-MM-DD' format. Defaults to 3 years ago.
        end (str, optional): End date in 'YYYY-MM-DD' format. Defaults to yesterday.
    
    Returns:
        pd.DataFrame: DataFrame with UTC datetime index and prime rate values
    """
    try:
        # Set default dates if not provided
        if start is None:
            start = (datetime.now() - pd.DateOffset(years=3)).strftime('%d/%m/%Y')
        else:
            start = pd.to_datetime(start).strftime('%d/%m/%Y')
            
        if end is None:
            end = (datetime.now() - pd.DateOffset(days=1)).strftime('%d/%m/%Y')
        else:
            end = pd.to_datetime(end).strftime('%d/%m/%Y')
        
        params = {
            'country': 'brazil',
            'start': start,
            'end': end,
            'sortBy': 'date',
            'sortOrder': 'desc'
        }
        
        response = make_request('/api/v2/prime-rate', params)
        
        # Check if response contains prime-rate data
        if isinstance(response, list) or (isinstance(response, dict) and 'prime-rate' in response):
            # Extract the data array
            data = response if isinstance(response, list) else response.get('prime-rate', [])
            
            if data:
                # Create DataFrame
                df = pd.DataFrame(data)
                
                # Convert date to UTC datetime
                df['date'] = pd.to_datetime(df['date'], format='%d/%m/%Y').dt.tz_localize('UTC')
                
                # Convert value to numeric, removing any % signs if present
                df['value'] = df['value'].replace({'%': ''}, regex=True)
                df['value'] = pd.to_numeric(df['value'], errors='coerce')
                
                # Set date as index with proper name
                df.set_index('date', inplace=True)
                df.index.name = 'date'
                
                # Sort index
                df.sort_index(inplace=True)
                
                # Drop any duplicate indices, keeping the last value
                df = df[~df.index.duplicated(keep='last')]
                
                # Drop epochDate column if it exists
                if 'epochDate' in df.columns:
                    df.drop('epochDate', axis=1, inplace=True)
                
                return df
                
        return None
        
    except Exception as e:
        print(f"Error fetching prime rate data: {str(e)}")
        return None

def get_available_currencies(search=None):
    """Get list of available currency pairs
    
    Args:
        search (str, optional): Search term to filter currencies
        
    Returns:
        list: Available currency pairs
    """
    params = {}
    if search:
        params['search'] = search
    return make_request('api/v2/currency/available', params)

def get_available_cryptos(search=None):
    """Get list of available cryptocurrencies
    
    Args:
        search (str, optional): Search term to filter cryptos
        
    Returns:
        list: Available cryptocurrency symbols
    """
    params = {}
    if search:
        params['search'] = search
    return make_request('api/v2/crypto/available', params)

def get_available_countries(search=None):
    """Get list of available countries for inflation/prime rate data
    
    Args:
        search (str, optional): Search term to filter countries
        
    Returns:
        list: Available country names
    """
    params = {}
    if search:
        params['search'] = search
    return make_request('api/v2/inflation/available', params)

def fetch_quote_open(tickers, range='1d', interval='1d'):
    """Fetch open prices for multiple tickers
    
    Args:
        tickers (str or list): Single ticker or list of ticker symbols
        range (str): Time range ('1d','5d','1mo','3mo','6mo','1y','2y','5y','10y','ytd','max')
        interval (str): Time interval ('1m','2m','5m','15m','30m','60m','90m','1h','1d','5d','1wk','1mo','3mo')
        
    Returns:
        pd.DataFrame: DataFrame with dates as index and tickers as columns containing open prices
    """
    try:
        if isinstance(tickers, str):
            tickers = [tickers]
            
        all_data = {}
        for ticker in tickers:
            ticker_sa = ticker.replace('.SA', '') + '.SA'
            
            params = {
                'range': range,
                'interval': interval
            }
            
            response = make_request(f'api/quote/{ticker_sa}', params)
            if not response:
                continue
                
            # Extract historical price data
            historical_data = None
            if isinstance(response, dict):
                historical_data = response.get('results', [{}])[0].get('historicalDataPrice', [])
            elif isinstance(response, list) and response:
                historical_data = response[0].get('historicalDataPrice', [])
                
            if historical_data:
                df = pd.DataFrame(historical_data)
                
                # Convert date based on interval
                intraday_intervals = ['1m', '2m', '5m', '15m', '30m', '60m', '90m', '1h']
                df['date'] = pd.to_datetime(df['date'], unit='s', utc=True)
                
                # Select only open price and set date as index
                df.set_index('date', inplace=True)
                all_data[ticker] = df['open']
                
        if all_data:
            df = pd.DataFrame(all_data)
            return _convert_to_float64(df)
        return pd.DataFrame()
        
    except Exception as e:
        print(f"Error fetching open prices: {str(e)}")
        return pd.DataFrame()

def fetch_quote_high(tickers, range='1d', interval='1d'):
    """Fetch high prices for multiple tickers"""
    try:
        if isinstance(tickers, str):
            tickers = [tickers]
            
        all_data = {}
        for ticker in tickers:
            ticker_sa = ticker.replace('.SA', '') + '.SA'
            
            params = {
                'range': range,
                'interval': interval
            }
            
            response = make_request(f'api/quote/{ticker_sa}', params)
            if not response:
                continue
                
            historical_data = None
            if isinstance(response, dict):
                historical_data = response.get('results', [{}])[0].get('historicalDataPrice', [])
            elif isinstance(response, list) and response:
                historical_data = response[0].get('historicalDataPrice', [])
                
            if historical_data:
                df = pd.DataFrame(historical_data)
                df['date'] = pd.to_datetime(df['date'], unit='s', utc=True)
                df.set_index('date', inplace=True)
                all_data[ticker] = df['high']
                
        if all_data:
            df = pd.DataFrame(all_data)
            return _convert_to_float64(df)
        return pd.DataFrame()
        
    except Exception as e:
        print(f"Error fetching high prices: {str(e)}")
        return pd.DataFrame()

def fetch_quote_low(tickers, range='1d', interval='1d'):
    """Fetch low prices for multiple tickers"""
    try:
        if isinstance(tickers, str):
            tickers = [tickers]
            
        all_data = {}
        for ticker in tickers:
            ticker_sa = ticker.replace('.SA', '') + '.SA'
            
            params = {
                'range': range,
                'interval': interval
            }
            
            response = make_request(f'api/quote/{ticker_sa}', params)
            if not response:
                continue
                
            historical_data = None
            if isinstance(response, dict):
                historical_data = response.get('results', [{}])[0].get('historicalDataPrice', [])
            elif isinstance(response, list) and response:
                historical_data = response[0].get('historicalDataPrice', [])
                
            if historical_data:
                df = pd.DataFrame(historical_data)
                df['date'] = pd.to_datetime(df['date'], unit='s', utc=True)
                df.set_index('date', inplace=True)
                all_data[ticker] = df['low']
                
        if all_data:
            df = pd.DataFrame(all_data)
            return _convert_to_float64(df)
        return pd.DataFrame()
        
    except Exception as e:
        print(f"Error fetching low prices: {str(e)}")
        return pd.DataFrame()

def fetch_quote_close(tickers, range='1d', interval='1d'):
    """Fetch close prices for multiple tickers"""
    try:
        if isinstance(tickers, str):
            tickers = [tickers]
            
        all_data = {}
        for ticker in tickers:
            ticker_sa = ticker.replace('.SA', '') + '.SA'
            
            params = {
                'range': range,
                'interval': interval
            }
            
            response = make_request(f'api/quote/{ticker_sa}', params)
            if not response:
                continue
                
            historical_data = None
            if isinstance(response, dict):
                historical_data = response.get('results', [{}])[0].get('historicalDataPrice', [])
            elif isinstance(response, list) and response:
                historical_data = response[0].get('historicalDataPrice', [])
                
            if historical_data:
                df = pd.DataFrame(historical_data)
                df['date'] = pd.to_datetime(df['date'], unit='s', utc=True)
                df.set_index('date', inplace=True)
                all_data[ticker] = df['close']
                
        if all_data:
            df = pd.DataFrame(all_data)
            return _convert_to_float64(df)
        return pd.DataFrame()
        
    except Exception as e:
        print(f"Error fetching close prices: {str(e)}")
        return pd.DataFrame()

def fetch_quote_volume(tickers, range='1d', interval='1d'):
    """Fetch volume data for multiple tickers"""
    try:
        if isinstance(tickers, str):
            tickers = [tickers]
            
        all_data = {}
        for ticker in tickers:
            ticker_sa = ticker.replace('.SA', '') + '.SA'
            
            params = {
                'range': range,
                'interval': interval
            }
            
            response = make_request(f'api/quote/{ticker_sa}', params)
            if not response:
                continue
                
            historical_data = None
            if isinstance(response, dict):
                historical_data = response.get('results', [{}])[0].get('historicalDataPrice', [])
            elif isinstance(response, list) and response:
                historical_data = response[0].get('historicalDataPrice', [])
                
            if historical_data:
                df = pd.DataFrame(historical_data)
                df['date'] = pd.to_datetime(df['date'], unit='s', utc=True)
                df.set_index('date', inplace=True)
                all_data[ticker] = df['volume']
                
        if all_data:
            df = pd.DataFrame(all_data)
            return _convert_to_float64(df)
        return pd.DataFrame()
        
    except Exception as e:
        print(f"Error fetching volume data: {str(e)}")
        return pd.DataFrame()

def extract_common_stock_data(bs_data_dict, stock_data_df):
    """Extract common stock data from balance sheet data and align with stock price dates
    
    Args:
        bs_data_dict (dict): Dictionary of balance sheet DataFrames by ticker
        stock_data_df (pd.DataFrame): DataFrame with stock price data and datetime index
        
    Returns:
        pd.DataFrame: DataFrame with common stock values aligned by date, tickers as columns
    """
    try:
        # Initialize dictionary to store common stock data
        common_stock_data = {}
        
        # Process each ticker's balance sheet data
        for ticker, bs_df in bs_data_dict.items():
            # Remove .SA suffix for matching if present
            clean_ticker = ticker.replace('.SA', '')
            
            # Check for variations of "Common Stock" in index
            possible_names = ['Commonstock', 'Common Stock', 'CommonStock']
            stock_row = None
            for name in possible_names:
                if name in bs_df.index:
                    stock_row = name
                    break
                    
            if stock_row is None:
                print(f"No common stock data found for {ticker}")
                continue
                
            # Extract common stock series and clean data
            common_stock_series = bs_df.loc[stock_row]
            
            # Convert values to float64, removing any formatting
            if isinstance(common_stock_series, pd.Series):
                common_stock_series = pd.to_numeric(
                    common_stock_series.astype(str).str.replace(',', ''), 
                    errors='coerce'
                ).astype('float64')
            
            # Convert index to datetime and localize to UTC
            common_stock_series.index = pd.to_datetime(common_stock_series.index)
            if common_stock_series.index.tz is None:
                common_stock_series.index = common_stock_series.index.tz_localize('UTC')
            
            # Store in dictionary using the same ticker format as stock_data_df
            ticker_key = f"{clean_ticker}.SA" if '.SA' in stock_data_df.columns[0] else clean_ticker
            common_stock_data[ticker_key] = common_stock_series
        
        if not common_stock_data:
            print("No common stock data found for any ticker")
            return pd.DataFrame()
            
        # Create DataFrame from all series
        result_df = pd.DataFrame(common_stock_data)
        
        # Ensure all columns match stock_data_df
        result_df = result_df[stock_data_df.columns]
        
        # Sort index
        result_df.sort_index(inplace=True)
        
        # Reindex to match stock_data_df dates and forward fill
        result_df = result_df.reindex(stock_data_df.index, method='ffill')
        
        # Ensure all values are float64
        result_df = result_df.astype('float64')
        
        # Print debug information
        print(f"\nData Summary:")
        print(f"Dates range: {result_df.index.min()} to {result_df.index.max()}")
        print(f"Columns (tickers): {result_df.columns.tolist()}")
        print(f"Data types: {result_df.dtypes.unique()}")
        print(f"\nSample data:")
        print(result_df.head())
        
        return result_df
        
    except Exception as e:
        print(f"Error extracting common stock data: {str(e)}")
        print(f"Error details: {str(e.__class__.__name__)}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()

