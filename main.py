import os
import json
import yfinance as yf
import pandas as pd
from pycoingecko import CoinGeckoAPI
from datetime import datetime

# Initialize CoinGecko API
cg = CoinGeckoAPI()

# Create directory structure
os.makedirs('crypto_data/crypto_top100', exist_ok=True)

# Column descriptions (same for all files)
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

# Get top 150 cryptocurrencies by market cap (to ensure we get 100 valid ones)
print("Fetching top 150 cryptocurrencies...")
top_coins = cg.get_coins_markets(vs_currency='usd', order='market_cap_desc', per_page=150, page=1)

# List to store file metadata and successful downloads
files_metadata = []
crypto_directory_data = []
successful_count = 0
failed_downloads = 0
TARGET_COUNT = 100

# Download data for each coin until we have 100 successful files
for i, coin in enumerate(top_coins, 1):
    if successful_count >= TARGET_COUNT:
        print(f"\n✓ Reached target of {TARGET_COUNT} cryptocurrency files!")
        break
    
    try:
        coin_name = coin['id']
        coin_symbol = coin['symbol'].upper()
        
        print(f"Processing {i}/150 (Successful: {successful_count}/{TARGET_COUNT}): {coin_name} ({coin_symbol})")
        
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
        
        # Save to CSV in crypto_top100 folder
        filename = f"{coin_name}_{coin_symbol}.csv"
        filepath = os.path.join('crypto_data', 'crypto_top100', filename)
        df.to_csv(filepath, index=False)
        
        # Increment successful counter
        successful_count += 1
        
        # Add to crypto directory
        crypto_directory_data.append({
            "Index": successful_count,
            "Crypto Name": str(coin_name),
            "File Name": str(filename)
        })
        
        # Create file metadata with descriptions for Kaggle
        file_description = f"Historical data for {coin_name} cryptocurrency"
        
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
            "path": f"crypto_top100/{filename}",
            "description": file_description,
            "columns": columns_metadata
        }
        files_metadata.append(file_meta)
        
        print(f"  ✓ Saved {filename} ({len(df)} rows)")
        
    except Exception as e:
        print(f"  ✗ Error processing {coin_name}: {str(e)}")
        failed_downloads += 1

# Create crypto_directory.csv (outside crypto_top100 folder)
print("\n" + "="*60)
print("Creating crypto_directory.csv...")
print("="*60)

crypto_dir_df = pd.DataFrame(crypto_directory_data)
crypto_dir_path = os.path.join('crypto_data', 'crypto_directory.csv')
crypto_dir_df.to_csv(crypto_dir_path, index=False)
print(f"✓ Created crypto_directory.csv with {len(crypto_dir_df)} cryptocurrencies")

# Add crypto_directory.csv to metadata
dir_columns = [
    {"name": "Index", "description": "Ranking index from 1 to 100 based on market capitalization"},
    {"name": "Crypto Name", "description": "Official cryptocurrency name (e.g., bitcoin, ethereum)"},
    {"name": "File Name", "description": "Corresponding CSV filename in crypto_top100 folder"}
]

dir_file_meta = {
    "path": "crypto_directory.csv",
    "description": "Directory listing of all 100 cryptocurrencies included in this dataset, ranked by market capitalization. Use this file to quickly find the filename for any cryptocurrency.",
    "columns": dir_columns
}
files_metadata.insert(0, dir_file_meta)  # Add at the beginning

# Create enhanced dataset description
print("\nCreating dataset metadata...")

dataset_description = f"""# 🪙 Top 100 Cryptocurrencies Historical Data (Automated)

## **Context**

The cryptocurrency market is volatile and fast-paced. A dataset that was accurate last month is already obsolete today.

This dataset solves the problem of "stale data." It is **automatically updated every week** via a scheduled pipeline to ensure you always have the most recent historical data for the **current Top 100 cryptocurrencies** by market capitalization.

Unlike massive, messy dump files, this dataset provides **clean, individual CSV files** for each coin in the `crypto_top100` folder, making it incredibly easy to load exactly what you need for your analysis.

## **Content**

The dataset contains **{successful_count} CSV files** organized in the `crypto_top100` folder. Each file represents a specific cryptocurrency (e.g., `bitcoin_BTC.csv`, `ethereum_ETH.csv`).

Each row in the files represents one day of trading history. The data spans from the coin's **earliest available listing** on Yahoo Finance up to the current week.

### **Key Features**

* **Survivorship Handling:** The pipeline fetches the *current* Top 100 list every week. If a new coin enters the top 100, its full history is automatically added.
* **Feature Engineering:** Includes pre-calculated technical indicators (SMA, Returns, Volatility) to save you time during preprocessing.
* **Granularity:** Daily (OHLCV).
* **Easy Navigation:** Use `crypto_directory.csv` to quickly find any cryptocurrency's filename.

## **Column Descriptions**

All CSV files contain the same 10 columns:

| Column Name | Data Type | Description |
| --- | --- | --- |
| **Date** | `Date` | The trading date (YYYY-MM-DD). |
| **Open** | `Float` | The price of the asset at the market open (00:00 UTC). |
| **High** | `Float` | The highest price reached during the trading day. |
| **Low** | `Float` | The lowest price reached during the trading day. |
| **Close** | `Float` | The price of the asset at the market close (23:59 UTC). |
| **Volume** | `Integer` | The total value of the asset traded during the day (in USD). |
| **Daily_Return** | `Float` | Percentage change in Close price compared to the previous day. Useful for analyzing ROI and risk. |
| **High_Low_Spread** | `Float` | The absolute difference between High and Low (`High - Low`). A direct measure of **intraday volatility**. |
| **SMA_7** | `Float` | **7-Day Simple Moving Average**. The average close price of the last 7 days. Useful for identifying short-term trends. |
| **SMA_30** | `Float` | **30-Day Simple Moving Average**. The average close price of the last 30 days. Useful for identifying medium-term trends. |

## **Dataset Structure**

```
crypto_data/
├── crypto_directory.csv          # Index of all 100 cryptocurrencies
└── crypto_top100/                 # Folder containing all crypto CSV files
    ├── bitcoin_BTC.csv
    ├── ethereum_ETH.csv
    ├── cardano_ADA.csv
    └── ... (100 files total)
```

## **Methodology**

This dataset is generated using a robust Python pipeline:

1. **Ranking:** The **CoinGecko API** is queried to determine the live ranking of the Top 150 coins by Market Cap.
2. **Extraction:** Detailed OHLCV data is fetched for these specific tickers using the **Yahoo Finance** library (`yfinance`).
3. **Selection:** The first 100 cryptocurrencies with available data are selected.
4. **Processing:** Technical indicators are computed using Pandas.
5. **Cleaning:** Data is cleaned and formatted for immediate use.

## **Inspiration & Use Cases**

This dataset is perfect for:

* **Price Forecasting:** Build LSTM or ARIMA models to predict future prices.
* **Correlation Analysis:** Which coins move together? Does BTC still lead the market?
* **Volatility Study:** Analyze how `High_Low_Spread` changes during bear vs. bull markets.
* **Backtesting Strategies:** Test trading strategies (e.g., "Buy when SMA_7 crosses above SMA_30") across 100 different assets instantly.
* **Portfolio Optimization:** Analyze risk-return profiles across multiple cryptocurrencies.

## **Acknowledgements**

* **CoinGecko:** For providing the comprehensive list of top cryptocurrencies.
* **Yahoo Finance:** For the historical market data.

## **Update Frequency**

**Weekly.** The pipeline runs automatically every Sunday at 00:00 UTC.

Last updated: {datetime.now().strftime('%Y-%m-%d')}

---

## **How to Use (Starter Code)**

### Load the cryptocurrency directory:

```python
import pandas as pd
import glob

# Load the directory to see all available cryptocurrencies
crypto_dir = pd.read_csv('crypto_directory.csv')
print(crypto_dir.head())
```

### Load a specific coin (e.g., Bitcoin):

```python
# Load Bitcoin data
btc_df = pd.read_csv('crypto_top100/bitcoin_BTC.csv')
print(btc_df.head())
```

### Load ALL files into a single DataFrame:

```python
# Load all cryptocurrency files
all_files = glob.glob("crypto_top100/*.csv")
full_market_df = pd.concat((pd.read_csv(f) for f in all_files), ignore_index=True)
print(full_market_df.head())
```
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
        "daily data",
        "top 100",
        "market cap"
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
print(f"✓ Successfully processed: {successful_count} cryptocurrencies")
print(f"✗ Failed/Skipped: {failed_downloads} cryptocurrencies")
print(f"✓ Total CSV files in crypto_top100/: {successful_count}")
print(f"✓ crypto_directory.csv created with {len(crypto_dir_df)} entries")
print(f"✓ Metadata saved with {len(files_metadata)} files")
print(f"✓ All data saved to crypto_data/ directory")
print(f"\n{'='*60}")
print("Dataset is ready for Kaggle upload!")
print(f"{'='*60}\n")