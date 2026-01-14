import os
import json
import yfinance as yf
import pandas as pd
from pycoingecko import CoinGeckoAPI
from datetime import datetime
import time

# --- CONFIGURATION ---
TARGET_COUNT = 100
FETCH_BUFFER = 200  # Fetch more than 100 to account for missing yfinance data
ROOT_DIR = 'crypto_dataset'
DATA_SUBDIR = 'crypto_top100'

# Initialize CoinGecko API
cg = CoinGeckoAPI()

# Create directory structure
# Final structure: crypto_dataset/crypto_top100/*.csv
csv_output_path = os.path.join(ROOT_DIR, DATA_SUBDIR)
os.makedirs(csv_output_path, exist_ok=True)

# Column descriptions (applied to every file)
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

# Get top cryptocurrencies (fetching buffer to ensure we hit 100 valid ones)
print(f"Fetching top {FETCH_BUFFER} cryptocurrencies to find {TARGET_COUNT} valid matches...")
try:
    top_coins = cg.get_coins_markets(vs_currency='usd', order='market_cap_desc', per_page=FETCH_BUFFER, page=1)
except Exception as e:
    print(f"Error fetching from CoinGecko: {e}")
    exit(1)

# Lists for metadata and directory
files_metadata = []
crypto_directory_list = []

successful_downloads = 0
processed_count = 0

print(f"\nStarting processing. Target: {TARGET_COUNT} files.\n")

for coin in top_coins:
    # Stop if we reached the target
    if successful_downloads >= TARGET_COUNT:
        break

    processed_count += 1
    coin_name = coin['id']
    coin_symbol = coin['symbol'].upper()
    
    # Construct ticker symbol for yfinance
    # some symbols might need specific formatting for yfinance, but usually SYMBOL-USD works
    ticker_symbol = f"{coin_symbol}-USD"
    
    try:
        # Download data from yfinance
        ticker = yf.Ticker(ticker_symbol)
        df = ticker.history(period="max", interval="1d")
        
        # VALIDATION: Check if data is empty or too short
        if df.empty or len(df) < 10:
            print(f"[{processed_count}/{FETCH_BUFFER}] ⚠️  Skipping {coin_name} ({ticker_symbol}): No data found on yfinance")
            continue
            
        # --- Feature Engineering ---
        df['Daily_Return'] = df['Close'].pct_change() * 100
        df['High_Low_Spread'] = df['High'] - df['Low']
        df['SMA_7'] = df['Close'].rolling(window=7).mean()
        df['SMA_30'] = df['Close'].rolling(window=30).mean()
        
        # Reset index to make Date a column
        df.reset_index(inplace=True)
        
        # Clean Date format if needed (remove timezone info if present)
        df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')
        
        # Select and reorder columns
        columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 
                   'Daily_Return', 'High_Low_Spread', 'SMA_7', 'SMA_30']
        df = df[columns]
        
        # --- Save CSV ---
        # File naming convention: coin_SYMBOL.csv
        filename = f"{coin_name}_{coin_symbol}.csv"
        # The physical path to save the file
        filepath = os.path.join(csv_output_path, filename)
        df.to_csv(filepath, index=False)
        
        # Increment success counter
        successful_downloads += 1
        print(f"[{processed_count}/{FETCH_BUFFER}] ✓ Saved {filename} (Index: {successful_downloads})")
        
        # --- 1. Update Crypto Directory List ---
        # Format: Index, Crypto Name, File Name
        crypto_directory_list.append({
            "Index": successful_downloads,
            "Crypto Name": str(coin_name),
            "File Name": str(filename)
        })

        # --- 2. Update Metadata for this specific file ---
        # Note: path in metadata must be relative to the root of the dataset upload
        relative_path = f"{DATA_SUBDIR}/{filename}"
        
        # Build column metadata structure for this file
        schema_fields = []
        for col_name, col_desc in COLUMN_DESCRIPTIONS.items():
            schema_fields.append({
                "name": col_name,
                "description": col_desc,
                "type": "string" if col_name == "Date" else "number"
            })

        file_meta = {
            "path": relative_path,
            "description": f"Historical data for cryptocurrency in {filename}",
            "schema": {
                "fields": schema_fields
            }
        }
        files_metadata.append(file_meta)
        
        # Sleep briefly to avoid rate limiting
        time.sleep(0.2)

    except Exception as e:
        print(f"[{processed_count}/{FETCH_BUFFER}] ✗ Error processing {coin_name}: {str(e)}")

# --- Finalize: Create crypto_directory.csv ---
print(f"\nCreating crypto_directory.csv...")
directory_df = pd.DataFrame(crypto_directory_list)
directory_path = os.path.join(ROOT_DIR, 'crypto_directory.csv')
directory_df.to_csv(directory_path, index=False)

# Add directory file to metadata
files_metadata.append({
    "path": "crypto_directory.csv",
    "description": "Index of the top 100 cryptocurrencies included in this dataset, mapping Rank to Coin Name and File Name.",
    "schema": {
        "fields": [
            {"name": "Index", "description": "Rank of the coin (1-100)"},
            {"name": "Crypto Name", "description": "Full name of the cryptocurrency"},
            {"name": "File Name", "description": "Name of the corresponding CSV file"}
        ]
    }
})

# --- Finalize: Create dataset-metadata.json ---
print("Creating dataset-metadata.json...")

dataset_description = f"""# Top 100 Cryptocurrency Historical Data

This dataset contains daily OHLCV (Open, High, Low, Close, Volume) data for the top 100 cryptocurrencies by market capitalization.

## Structure
- `crypto_top100/`: Folder containing 100 individual CSV files.
- `crypto_directory.csv`: A reference list mapping the Index (1-100) to the Coin Name and File Name.

## Data Source
Data is sourced from Yahoo Finance using the yfinance Python library.
Updated automatically via GitHub Actions.

Last updated: {datetime.now().strftime('%Y-%m-%d')}
"""

dataset_metadata = {
    "title": "Top 100 Cryptocurrency Historical Data (Automated)",
    "id": f"{os.environ.get('KAGGLE_USERNAME', 'your-username')}/top-100-cryptocurrency-historical-data",
    "licenses": [{"name": "other"}],
    "description": dataset_description,
    "keywords": ["cryptocurrency", "bitcoin", "finance", "time series", "technical analysis"],
    "resources": files_metadata
}

metadata_path = os.path.join(ROOT_DIR, 'dataset-metadata.json')
with open(metadata_path, 'w') as f:
    json.dump(dataset_metadata, f, indent=2)

print("\n" + "="*60)
print(f"DONE! Processed {successful_downloads} coins.")
print(f"Data saved to: {ROOT_DIR}/")
print("="*60)