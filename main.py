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

# List to store file metadata
files_metadata = []

# Download data for each coin
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
            f"Data sourced from Yahoo Finance with maximum available history."
        )
        
        # Build columns metadata with descriptions
        columns_metadata = []
        for col in columns:
            col_info = {
                "name": col,
                "description": COLUMN_DESCRIPTIONS.get(col, f"{col} data")
            }
            columns_metadata.append(col_info)
        
        # Add file metadata
        file_meta = {
            "path": filename,
            "description": file_description,
            "columns": columns_metadata
        }
        files_metadata.append(file_meta)
        
        print(f"  ✓ Saved {filename} ({len(df)} rows)")
        
    except Exception as e:
        print(f"  ✗ Error processing {coin_name}: {str(e)}")

# Create dataset metadata
print("\nCreating dataset metadata...")

dataset_metadata = {
    "title": "Top 100 Cryptocurrency Historical Data (Automated)",
    "id": f"{os.environ.get('KAGGLE_USERNAME', 'your-username')}/top-100-cryptocurrency-historical-data",
    "licenses": [{"name": "other"}],
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
        "market data"
    ],
    "resources": files_metadata
}

# Save metadata to JSON
metadata_path = os.path.join('crypto_data', 'dataset-metadata.json')
with open(metadata_path, 'w') as f:
    json.dump(dataset_metadata, f, indent=2)

print(f"✓ Metadata saved with {len(files_metadata)} files")
print(f"✓ All data saved to crypto_data/ directory")
print("\nDataset is ready for Kaggle upload!")