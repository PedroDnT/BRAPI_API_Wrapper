
<h3 align="center">BR_API Wrapper</h3>

<div align="center">

[![Status](https://img.shields.io/badge/status-active-success.svg)]()
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](/LICENSE)

</div>

---

<p align="center">
A Python module that wraps the BRAPI API and BCB data services to provide easy access to Brazilian financial market data.
<br> 
</p>

## About <a name = "about"></a>

This module provides functions to access Brazilian financial market data through the BRAPI API and Brazilian Central Bank (BCB) services. It handles API authentication, request formatting, and returns clean pandas DataFrames ready for analysis.

## 🏁 Getting Started <a name = "getting_started"></a>

### Prerequisites

```bash
pip install pandas numpy requests python-dotenv python-bcb
```

### Environment Setup

Create a `.env` file in your project root with your BRAPI API token:
```bash
BRAPI_TOKEN='your_api_token_here'
```

## 🎈 Usage <a name="usage"></a>

Import the functions you need:

```python
from brapi_wrapper import (
    make_request,
    fetch_stock_data,
    fetch_inflation,
    fetch_prime_rate,
    calculate_historical_metrics,
    calculate_market_metrics,
    get_available_currencies,
    get_available_cryptos,
    get_available_countries
)
```

### Available Functions

#### Data Fetching
```python
# Fetch stock data
stock_data = fetch_stock_data('PETR4.SA')

# Get inflation data
inflation = fetch_inflation()

# Get prime rate (SELIC)
selic = fetch_prime_rate()
```

#### Market Metrics
```python
# Calculate historical metrics
hist_metrics = calculate_historical_metrics(['PETR4.SA', 'VALE3.SA'])

# Calculate market metrics
market_metrics = calculate_market_metrics(['PETR4.SA', 'VALE3.SA'])
```

#### Available Items Lists
```python
# Get available currencies
currencies = get_available_currencies()

# Get available cryptocurrencies
cryptos = get_available_cryptos()

# Get available countries
countries = get_available_countries()
```

### Response Formats

#### Stock Data
```python
{
    'ticker': str,
    'prices': pd.DataFrame,  # Daily price data
    'volume': pd.DataFrame   # Daily volume data
}
```

#### Market Metrics
```python
{
    'ticker': {
        'metrics': pd.DataFrame,  # Financial metrics
        'descriptions': dict,     # Metric descriptions
        'period': str            # Data period
    }
}
```

## ⛏️ Dependencies <a name = "built_using"></a>

- pandas
- numpy
- requests
- python-dotenv
- python-bcb

## Data Sources <a name = "acknowledgment"></a>

- [BRAPI](https://brapi.dev) - Brazilian stocks and market data
- [BCB](https://www.bcb.gov.br/) - Brazilian Central Bank macroeconomic data

## 📝 License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details.
