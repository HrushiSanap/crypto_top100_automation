import os
import json
import yfinance as yf
import pandas as pd
from pycoingecko import CoinGeckoAPI
from datetime import datetime

# Initialize CoinGecko API
cg = CoinGeckoAPI()

# Create directory for crypto data
os.makedirs('crypto_data', exist_ok=True)

# Get top 100 cryptocurrencies by market cap
print("Fetching top 100 cryptocurrencies...")
top_coins = cg.get_coins_markets(vs_currency='usd', order='market_cap_desc', per_page=100, page=1)

# Column descriptions for metadata
COLUMN_DESCRIPTIONS = {
    "Date": "The trading date (YYYY-MM-DD format)",
    "Open": "Price at the start of the day (00:00 UTC) in USD",
    "High": "Highest price reached during the day in USD",
    "Low": "Lowest price reached during the day in USD",
    "Close": "Price at the end of the day (23:59 UTC) in USD",
    "Volume": "Total trading volume in USD",
    "Daily_Return": "Percentage change from the previous day's close",
    "High_Low_Spread": "Intraday volatility calculated as (High - Low)",
    "SMA_7": "Simple Moving Average over 7 days (7-day trend)",
    "SMA_30": "Simple Moving Average over 30 days (30-day trend)"
}

# Create data dictionary CSV file
print("\nCreating data_dictionary.csv...")
data_dict_df = pd.DataFrame([
    {"Column Name": col, "Description": desc, "Data Type": "string" if col == "Date" else "float"}
    for col, desc in COLUMN_DESCRIPTIONS.items()
])
data_dict_path = os.path.join('crypto_data', 'data_dictionary.csv')
data_dict_df.to_csv(data_dict_path, index=False)
print(f"✓ Created data_dictionary.csv with {len(data_dict_df)} columns documented")

# List to store file metadata
files_metadata = []

# Add data dictionary to metadata first
dict_file_meta = {
    "path": "data_dictionary.csv",
    "description": "Data dictionary explaining all columns present in the cryptocurrency CSV files. Each row describes a column name, its meaning, and data type."
}
files_metadata.append(dict_file_meta)

# Download data for each coin
successful_downloads = 0
failed_downloads = 0

for i, coin in enumerate(top_coins, 1):
    try:
        coin_name = coin['id']
        coin_symbol = coin['symbol'].upper()
        
        print(f"Processing {i}/100: {coin_name} ({coin_symbol})")
        
        # Construct ticker symbol for yfinance
        ticker_symbol = f"{coin_symbol}-USD"
        
        # Download data from yfinance
        ticker = yf.Ticker(ticker_symbol)
        df = ticker.history(period="max", interval="1d")
        
        if df.empty:
            print(f"  ⚠️ No data available for {coin_name}")
            failed_downloads += 1
            continue
        
        # Calculate additional features
        df['Daily_Return'] = df['Close'].pct_change() * 100
        df['High_Low_Spread'] = df['High'] - df['Low']
        df['SMA_7'] = df['Close'].rolling(window=7).mean()
        df['SMA_30'] = df['Close'].rolling(window=30).mean()
        
        # Reset index to make Date a column
        df.reset_index(inplace=True)
        
        # Select and reorder columns
        columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 
                   'Daily_Return', 'High_Low_Spread', 'SMA_7', 'SMA_30']
        df = df[columns]
        
        # Save to CSV
        filename = f"{coin_name}_{coin_symbol}.csv"
        filepath = os.path.join('crypto_data', filename)
        df.to_csv(filepath, index=False)
        
        # Create file metadata with descriptions
        file_description = (
            f"Historical daily OHLCV data for {coin_name} ({coin_symbol}). "
            f"Includes price data (Open, High, Low, Close), trading volume, "
            f"and calculated technical indicators (Daily Returns, Moving Averages, Volatility). "
            f"Data sourced from Yahoo Finance with maximum available history. "
            f"See data_dictionary.csv for detailed column descriptions."
        )
        
        # Add file metadata (no need for column descriptions since we have data_dictionary.csv)
        file_meta = {
            "path": filename,
            "description": file_description
        }
        files_metadata.append(file_meta)
        
        successful_downloads += 1
        print(f"  ✓ Saved {filename} ({len(df)} rows)")
        
    except Exception as e:
        print(f"  ✗ Error processing {coin_name}: {str(e)}")
        failed_downloads += 1

# Create dataset metadata
print("\n" + "="*60)
print("Creating dataset metadata...")
print("="*60)

dataset_description = f"""# Top 100 Cryptocurrency Historical Data

This dataset contains daily OHLCV (Open, High, Low, Close, Volume) data for the top 100 cryptocurrencies by market capitalization.

## Dataset Contents

- **{successful_downloads} cryptocurrency CSV files** - One file per coin with complete historical data
- **1 data dictionary file** - Complete documentation of all columns

## Column Information

All cryptocurrency CSV files contain the same 10 columns. For detailed descriptions, please refer to `data_dictionary.csv`.

Quick overview:
- **Price Data**: Open, High, Low, Close (USD)
- **Volume**: Total trading volume (USD)
- **Technical Indicators**: Daily Returns, High-Low Spread, 7-day SMA, 30-day SMA

## Data Source

- Data is sourced from Yahoo Finance using the yfinance Python library
- Maximum available history for each cryptocurrency
- Updated automatically every week via GitHub Actions

## File Naming Convention

Files are named as: `{{coin_name}}_{{SYMBOL}}.csv`

Examples:
- `bitcoin_BTC.csv`
- `ethereum_ETH.csv`
- `cardano_ADA.csv`

## Usage

```python
import pandas as pd

# Load data for Bitcoin
btc = pd.read_csv('bitcoin_BTC.csv')

# Load data dictionary
data_dict = pd.read_csv('data_dictionary.csv')
```

## Updates

This dataset is automatically updated weekly to reflect the current top 100 cryptocurrencies by market cap.

Last updated: {datetime.now().strftime('%Y-%m-%d')}
"""

dataset_metadata = {
    "title": "Top 100 Cryptocurrency Historical Data (Automated)",
    "id": f"{os.environ.get('KAGGLE_USERNAME', 'your-username')}/top-100-cryptocurrency-historical-data",
    "licenses": [{"name": "other"}],
    "description": dataset_description,
    "keywords": [
        "cryptocurrencies",
        "cryptocurrency",
        "bitcoin",
        "ethereum",
        "finance",
        "time series analysis",
        "trading",
        "technical analysis",
        "automated",
        "market data",
        "ohlcv",
        "daily data"
    ],
    "resources": files_metadata
}

# Save metadata to JSON
metadata_path = os.path.join('crypto_data', 'dataset-metadata.json')
with open(metadata_path, 'w') as f:
    json.dump(dataset_metadata, f, indent=2)

# Print summary
print(f"\n{'='*60}")
print("SUMMARY")
print(f"{'='*60}")
print(f"✓ Successfully processed: {successful_downloads} cryptocurrencies")
print(f"✗ Failed: {failed_downloads} cryptocurrencies")
print(f"✓ Total files created: {successful_downloads + 1} (including data_dictionary.csv)")
print(f"✓ Metadata saved with {len(files_metadata)} files")
print(f"✓ All data saved to crypto_data/ directory")
print(f"\n{'='*60}")
print("Dataset is ready for Kaggle upload!")
print(f"{'='*60}\n")