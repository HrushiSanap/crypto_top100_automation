import pandas as pd
import yfinance as yf
from pycoingecko import CoinGeckoAPI
import os
import time
import shutil
import json

# --- Configuration ---
output_folder = 'crypto_data'
cg = CoinGeckoAPI()

# --- Step 0: Clean/Create Output Folder ---
if os.path.exists(output_folder):
    shutil.rmtree(output_folder)
os.makedirs(output_folder)

# --- Define the Column Descriptions  ---
# This schema applies to ALL files.
common_fields = [
    {'name': 'Date', 'description': 'The trading date in YYYY-MM-DD format.'},
    {'name': 'Open', 'description': 'The price of the asset at the opening of the trading day (00:00 UTC).'},
    {'name': 'High', 'description': 'The highest price reached during the trading day.'},
    {'name': 'Low', 'description': 'The lowest price reached during the trading day.'},
    {'name': 'Close', 'description': 'The price of the asset at the close of the trading day (23:59 UTC).'},
    {'name': 'Volume', 'description': 'The total number of units traded multiplied by the price (in USD).'},
    {'name': 'Daily_Return', 'description': 'Percentage change in the Close price compared to the previous day.'},
    {'name': 'High_Low_Spread', 'description': 'Difference between High and Low prices (Intraday Volatility).'},
    {'name': 'SMA_7', 'description': '7-Day Simple Moving Average (Short-term trend).'},
    {'name': 'SMA_30', 'description': '30-Day Simple Moving Average (Medium-term trend).'},
    {'name': 'Symbol', 'description': 'The ticker symbol of the cryptocurrency.'},
    {'name': 'Name', 'description': 'The full name of the cryptocurrency.'}
]

# Initialize Metadata Structure
metadata = {
    "title": "Top 100 Cryptocurrency Historical Data",
    "id": f"{os.environ.get('KAGGLE_USERNAME')}/top-100-cryptocurrency-historical-data",
    "licenses": [{"name": "CC0-1.0"}],
    "description": "Weekly updated dataset of the top 100 cryptocurrencies by market cap, including OHLCV data and technical indicators.",
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
        
        # Define filename
        file_name = f"{name.replace(' ', '_').lower()}_{symbol}.csv"
        file_path = f"{output_folder}/{file_name}"
        
        # Save CSV
        df.to_csv(file_path, index=False)
        
        # --- ADD RESOURCE TO METADATA ---
        # This tells Kaggle exactly what this specific file is
        resource_entry = {
            "path": file_name,
            "description": f"Historical OHLCV data and technical indicators for {name} ({symbol}).",
            "schema": {
                "fields": common_fields
            }
        }
        metadata["resources"].append(resource_entry)
        
        time.sleep(0.2)
        
    except Exception as e:
        print(f"Error: {e}")

# --- Step 3: Write the Metadata File ---
with open(f"{output_folder}/dataset-metadata.json", 'w') as f:
    json.dump(metadata, f, indent=4)

print("Data collection complete. Metadata with column descriptions generated.")