import pandas as pd
import yfinance as yf
from pycoingecko import CoinGeckoAPI
import os
import time
import shutil
import json
import traceback

# --- Configuration ---
output_folder = 'crypto_data'
cg = CoinGeckoAPI()
# Get username from environment variable (GitHub Secrets) or use a placeholder for local testing
kaggle_username = os.environ.get('KAGGLE_USERNAME', 'YOUR_USERNAME_HERE')

# --- Step 0: Clean/Create Output Folder ---
if os.path.exists(output_folder):
    shutil.rmtree(output_folder)
os.makedirs(output_folder)

# --- Define the Column Descriptions & Types ---
# This is the "Master List". We will filter this list for each specific file later.
common_fields = [
    {'name': 'Date', 'description': 'The trading date in YYYY-MM-DD format.', 'type': 'date'},
    {'name': 'Open', 'description': 'The price of the asset at the opening of the trading day (00:00 UTC).', 'type': 'number'},
    {'name': 'High', 'description': 'The highest price reached during the trading day.', 'type': 'number'},
    {'name': 'Low', 'description': 'The lowest price reached during the trading day.', 'type': 'number'},
    {'name': 'Close', 'description': 'The price of the asset at the close of the trading day (23:59 UTC).', 'type': 'number'},
    {'name': 'Volume', 'description': 'The total number of units traded multiplied by the price (in USD).', 'type': 'number'},
    {'name': 'Daily_Return', 'description': 'Percentage change in the Close price compared to the previous day.', 'type': 'number'},
    {'name': 'High_Low_Spread', 'description': 'Difference between High and Low prices (Intraday Volatility).', 'type': 'number'},
    {'name': 'SMA_7', 'description': '7-Day Simple Moving Average (Short-term trend).', 'type': 'number'},
    {'name': 'SMA_30', 'description': '30-Day Simple Moving Average (Medium-term trend).', 'type': 'number'},
    {'name': 'Symbol', 'description': 'The ticker symbol of the cryptocurrency.', 'type': 'string'},
    {'name': 'Name', 'description': 'The full name of the cryptocurrency.', 'type': 'string'}
]

# Initialize Metadata Structure
# IMPORTANT: Ensure 'id' matches your existing Kaggle dataset URL exactly.
metadata = {
    "title": "Top 100 Cryptocurrency Historical Data",
    "subtitle": "Daily OHLCV + Technical Indicators for Top 100 Crypto (Auto-Updated)",
    "description": "### Context\nThis dataset contains historical daily data for the current top 100 cryptocurrencies by market cap.\n\n### Content\nEach CSV file represents one coin. Data includes Open, High, Low, Close, Volume, and calculated technical indicators.",
    "id": f"{kaggle_username}/top-100-cryptocurrency-historical-data",
    "licenses": [{"name": "CC0-1.0"}],
    "resources": [] 
}

# --- Step 1: Get Top 100 ---
print("Fetching Top 100 list...")
try:
    top_coins = cg.get_coins_markets(vs_currency='usd', order='market_cap_desc', per_page=100, page=1)
except Exception as e:
    print(f"Failed to fetch Top 100: {e}")
    exit(1)

# --- Step 2: Download Data & Build Metadata ---
print("Starting data download. Press Ctrl+C to stop early (metadata will still be saved).")

try:
    for i, coin in enumerate(top_coins):
        symbol = coin['symbol'].upper()
        name = coin['name']
        
        # Mapping overrides for yfinance tickers
        ticker_symbol = f"{symbol}-USD"
        if symbol == 'MIOTA': ticker_symbol = 'IOTA-USD' 
        if symbol == 'BNB': ticker_symbol = 'BNB-USD'

        print(f"[{i+1}/100] {name} ({ticker_symbol})...")
        
        try:
            ticker = yf.Ticker(ticker_symbol)
            df = ticker.history(period="max")
            
            if df.empty:
                print(f"   -> No data found for {ticker_symbol}")
                continue
                
            df.reset_index(inplace=True)
            
            # 1. Clean Columns
            # Ensure we only keep columns that yfinance actually returned
            available_cols = [c for c in ['Date', 'Open', 'High', 'Low', 'Close', 'Volume'] if c in df.columns]
            df = df[available_cols]
            
            # 2. Format Date
            if 'Date' in df.columns:
                df['Date'] = df['Date'].dt.date
            
            # 3. Calculate Indicators (Only if column exists AND enough data points)
            if 'Close' in df.columns:
                df['Daily_Return'] = df['Close'].pct_change() * 100
                
                if len(df) >= 7:
                    df['SMA_7'] = df['Close'].rolling(window=7).mean()
                if len(df) >= 30:
                    df['SMA_30'] = df['Close'].rolling(window=30).mean()
            
            if 'High' in df.columns and 'Low' in df.columns:
                df['High_Low_Spread'] = df['High'] - df['Low']
            
            # Add Identifiers
            df['Symbol'] = symbol
            df['Name'] = name
            
            # 4. Save CSV
            # Create a safe filename (remove spaces/special chars)
            safe_name = "".join([c if c.isalnum() else "_" for c in name])
            file_name = f"{safe_name.lower()}_{symbol}.csv"
            file_path = os.path.join(output_folder, file_name)
            
            df.to_csv(file_path, index=False)
            
            # 5. DYNAMIC METADATA GENERATION (The Critical Fix)
            # Only add descriptions for columns that exist in THIS specific dataframe.
            # This prevents Kaggle API errors when a new coin lacks SMA_30 history.
            actual_columns = df.columns.tolist()
            
            file_specific_schema = [
                field for field in common_fields 
                if field['name'] in actual_columns
            ]

            resource_entry = {
                "path": file_name,
                "description": f"Historical OHLCV data and technical indicators for {name} ({symbol}).",
                "schema": {
                    "fields": file_specific_schema
                }
            }
            metadata["resources"].append(resource_entry)
            
            time.sleep(0.2) # Respect rate limits
            
        except Exception as e:
            print(f"   -> Error processing {name}: {e}")

except KeyboardInterrupt:
    print("\nProcess interrupted by user. Saving metadata for downloaded files...")
except Exception as e:
    print(f"\nCritical error in loop: {e}")
    traceback.print_exc()

finally:
    # --- Step 3: Write the Metadata File (Always runs) ---
    print("\nGenerating dataset-metadata.json...")
    meta_path = os.path.join(output_folder, 'dataset-metadata.json')
    with open(meta_path, 'w', encoding='utf-8') as f:
        json.dump(metadata, f, indent=4)
    
    print(f"Complete. Metadata generated for {len(metadata['resources'])} files.")
