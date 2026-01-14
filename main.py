import os
import json
import yfinance as yf
import pandas as pd
from pycoingecko import CoinGeckoAPI
from datetime import datetime
import time

# --- CONFIGURATION ---
TARGET_COUNT = 100
FETCH_BUFFER = 200  # Fetch extra to account for missing data
ROOT_DIR = 'crypto_dataset'
DATA_SUBDIR = 'crypto_top100'

# Initialize CoinGecko API
cg = CoinGeckoAPI()

# Create directory structure
# Final structure: crypto_dataset/crypto_top100/*.csv
csv_output_path = os.path.join(ROOT_DIR, DATA_SUBDIR)
os.makedirs(csv_output_path, exist_ok=True)

# Column descriptions
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

# Fetch top coins
print(f"Fetching top {FETCH_BUFFER} cryptocurrencies...")
try:
    top_coins = cg.get_coins_markets(vs_currency='usd', order='market_cap_desc', per_page=FETCH_BUFFER, page=1)
except Exception as e:
    print(f"Error fetching from CoinGecko: {e}")
    exit(1)

# Lists for metadata tracking
files_metadata = []
crypto_directory_list = []

successful_downloads = 0
processed_count = 0

print(f"\nStarting processing. Target: {TARGET_COUNT} files.\n")

for coin in top_coins:
    if successful_downloads >= TARGET_COUNT:
        break

    processed_count += 1
    coin_name = coin['id']
    coin_symbol = coin['symbol'].upper()
    ticker_symbol = f"{coin_symbol}-USD"
    
    try:
        # Download data
        ticker = yf.Ticker(ticker_symbol)
        df = ticker.history(period="max", interval="1d")
        
        if df.empty or len(df) < 10:
            print(f"[{processed_count}/{FETCH_BUFFER}] ⚠️  Skipping {coin_name}: No data")
            continue
            
        # Feature Engineering
        df['Daily_Return'] = df['Close'].pct_change() * 100
        df['High_Low_Spread'] = df['High'] - df['Low']
        df['SMA_7'] = df['Close'].rolling(window=7).mean()
        df['SMA_30'] = df['Close'].rolling(window=30).mean()
        
        df.reset_index(inplace=True)
        df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')
        
        columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 
                   'Daily_Return', 'High_Low_Spread', 'SMA_7', 'SMA_30']
        df = df[columns]
        
        # Save CSV
        filename = f"{coin_name}_{coin_symbol}.csv"
        filepath = os.path.join(csv_output_path, filename)
        df.to_csv(filepath, index=False)
        
        successful_downloads += 1
        print(f"[{processed_count}/{FETCH_BUFFER}] ✓ Saved {filename}")
        
        # Track for directory
        crypto_directory_list.append({
            "Index": successful_downloads,
            "Crypto Name": str(coin_name),
            "File Name": str(filename)
        })

        time.sleep(0.1)

    except Exception as e:
        print(f"[{processed_count}/{FETCH_BUFFER}] ✗ Error {coin_name}: {str(e)}")

# --- Create Directory File ---
print(f"\nCreating crypto_directory.csv...")
directory_df = pd.DataFrame(crypto_directory_list)
directory_path = os.path.join(ROOT_DIR, 'crypto_directory.csv')
directory_df.to_csv(directory_path, index=False)


# --- METADATA GENERATION (FIXED) ---
print("Generating metadata...")

def get_clean_path(folder, filename):
    # Enforce forward slashes for JSON compatibility
    path = os.path.join(folder, filename)
    return path.replace("\\", "/")

# 1. Generate Metadata for Coin CSVs
for file_info in crypto_directory_list:
    filename = file_info["File Name"]
    # Path relative to dataset root
    clean_path = f"{DATA_SUBDIR}/{filename}" 
    
    # "Safety Net": Add column descriptions to the text description
    col_text_list = "\n".join([f"- **{k}**: {v}" for k, v in COLUMN_DESCRIPTIONS.items()])
    
    file_desc = (
        f"Historical OHLCV data for **{file_info['Crypto Name']}**.\n\n"
        f"### Column Definitions\n{col_text_list}"
    )
    
    # Strict Schema
    schema_fields = []
    for col, desc in COLUMN_DESCRIPTIONS.items():
        dtype = "string" if col == "Date" else "number"
        schema_fields.append({"name": col, "description": desc, "type": dtype})

    files_metadata.append({
        "path": clean_path,
        "description": file_desc,
        "schema": {"fields": schema_fields}
    })

# 2. Generate Metadata for Directory File
dir_desc = (
    "Index file mapping Rank (1-100) to Coin Name and Filename.\n\n"
    "### Columns\n"
    "- **Index**: Rank by market cap (1-100)\n"
    "- **Crypto Name**: Name of the coin\n"
    "- **File Name**: Name of the CSV file in crypto_top100 folder"
)

files_metadata.append({
    "path": "crypto_directory.csv",
    "description": dir_desc,
    "schema": {
        "fields": [
            {"name": "Index", "description": "Rank (1-100)", "type": "integer"},
            {"name": "Crypto Name", "description": "Coin Name", "type": "string"},
            {"name": "File Name", "description": "Filename", "type": "string"}
        ]
    }
})

# 3. Create JSON
dataset_metadata = {
    "title": "Top 100 Cryptocurrency Historical Data (Automated)",
    "id": f"{os.environ.get('KAGGLE_USERNAME')}/top-100-cryptocurrency-historical-data",
    "licenses": [{"name": "other"}],
    "description": f"Automated weekly update of top 100 cryptos.\nLast updated: {datetime.now().strftime('%Y-%m-%d')}",
    "keywords": ["crypto", "finance", "time series"],
    "resources": files_metadata
}

with open(os.path.join(ROOT_DIR, 'dataset-metadata.json'), 'w') as f:
    json.dump(dataset_metadata, f, indent=2)

print("✓ Metadata generation complete.")