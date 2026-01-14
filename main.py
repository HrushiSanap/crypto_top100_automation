import pandas as pd
import yfinance as yf
from pycoingecko import CoinGeckoAPI
import os
import time
import shutil
import json

# --- Configuration ---
output_folder = 'crypto_data' # Folder name for Kaggle
cg = CoinGeckoAPI()

# --- Step 0: Clean/Create Output Folder ---
if os.path.exists(output_folder):
    shutil.rmtree(output_folder)
os.makedirs(output_folder)

# --- Step 1: Get Top 100 ---
print("Fetching Top 100 list...")
try:
    top_coins = cg.get_coins_markets(vs_currency='usd', order='market_cap_desc', per_page=100, page=1)
except Exception as e:
    print(f"Failed to fetch Top 100: {e}")
    exit(1)

# --- Step 2: Download Data ---
for i, coin in enumerate(top_coins):
    symbol = coin['symbol'].upper()
    name = coin['name']
    ticker_symbol = f"{symbol}-USD"
    
    # Overrides
    if symbol == 'MIOTA': ticker_symbol = 'IOTA-USD' 
    if symbol == 'BNB': ticker_symbol = 'BNB-USD'

    print(f"[{i+1}/100] {name} ({ticker_symbol})...")
    
    try:
        ticker = yf.Ticker(ticker_symbol)
        df = ticker.history(period="max")
        
        if df.empty:
            continue
            
        df.reset_index(inplace=True)
        df = df[['Date', 'Open', 'High', 'Low', 'Close', 'Volume']]
        df['Date'] = df['Date'].dt.date
        
        # Technical Indicators
        df['Daily_Return'] = df['Close'].pct_change() * 100
        df['High_Low_Spread'] = df['High'] - df['Low']
        df['SMA_7'] = df['Close'].rolling(window=7).mean()
        df['SMA_30'] = df['Close'].rolling(window=30).mean()
        
        df['Symbol'] = symbol
        df['Name'] = name
        
        filename = f"{output_folder}/{name.replace(' ', '_').lower()}_{symbol}.csv"
        df.to_csv(filename, index=False)
        time.sleep(0.2)
        
    except Exception as e:
        print(f"Error: {e}")

# --- Step 3: Create Kaggle Metadata ---
# This file is REQUIRED for the API to know where to upload
metadata = {
    "title": "Top 100 Cryptocurrency Historical Data",
    "id": f"{os.environ.get('KAGGLE_USERNAME')}/top-100-cryptocurrency-historical-data",
    "licenses": [{"name": "CC0-1.0"}],
    "isPrivate": False
}

with open(f"{output_folder}/dataset-metadata.json", 'w') as f:
    json.dump(metadata, f)

print("Data collection complete. Metadata generated.")