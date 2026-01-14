# 🚀 Top 100 Cryptocurrency Historical Data (Automated)

![Python](https://img.shields.io/badge/Python-3.9%2B-blue?logo=python&logoColor=white)  
![License](https://img.shields.io/badge/license-Unlicense-green)  
![Status](https://img.shields.io/badge/Status-Active-success)  
![Kaggle](https://img.shields.io/badge/Kaggle-Dataset-20BEFF?logo=kaggle&logoColor=white)

**An automated data pipeline that generates specific historical datasets for the Top 100 Cryptocurrencies by Market Cap.**

Every week, this repository fetches the freshest ranking of coins, downloads their full daily OHLCV history, calculates technical indicators, and uploads the clean CSV files to [Kaggle](https://www.kaggle.com/).

🔗 **[View the Live Dataset on Kaggle](https://www.kaggle.com/YOUR_USERNAME_HERE/top-100-cryptocurrency-historical-data)** *(Replace the link above with your actual Kaggle Dataset URL)*

---

## 📊 What This Does

This project solves the problem of "stale" crypto datasets. Instead of a static snapshot, this repo uses **GitHub Actions** to run a weekly cron job that:

1.  **Identifies the Giants:** Uses the **CoinGecko API** to identify the current top 100 coins by market cap (dynamically handling new entrants and dropouts).  
2.  **Fetches Granular Data:** Uses **Yahoo Finance (`yfinance`)** to download the maximum available daily history for each coin.  
3.  **Engineers Features:** Calculates key technical analysis indicators (Volatility, Moving Averages, Returns).  
4.  **Updates Kaggle:** Automatically versions and pushes the new data to Kaggle using their official API.

---

## 📂 Data Structure

The output consists of 100 separate CSV files (one per coin), making it easy to load specific assets.

### File Naming Convention  
`{coin_name}_{symbol}.csv`    
*Example: `bitcoin_BTC.csv`, `ethereum_ETH.csv`*

### Column Dictionary  
Each CSV contains the following columns:

| Column Name | Description |  
| :--- | :--- |  
| **Date** | The trading date (YYYY-MM-DD). |  
| **Open** | Price at the start of the day (00:00 UTC). |  
| **High** | Highest price reached during the day. |  
| **Low** | Lowest price reached during the day. |  
| **Close** | Price at the end of the day (23:59 UTC). |  
| **Volume** | Total trading volume in USD. |  
| **Daily_Return** | Percentage change from the previous day's close. |  
| **High_Low_Spread** | Intraday volatility (High - Low). |  
| **SMA_7** | Simple Moving Average (7-day trend). |  
| **SMA_30** | Simple Moving Average (30-day trend). |

---

## 🛠️ How It Works (The Pipeline)

The automation is handled by `.github/workflows/update_data.yml`.

1.  **Trigger:** Runs automatically at **00:00 UTC every Sunday**.  
2.  **Environment:** Spins up an Ubuntu runner.  
3.  **Execution:**  
    * Runs `main.py`.  
    * Cleans the `crypto_data` directory (removes old files).  
    * Iterates through the Top 100 list.  
    * Downloads data and saves CSVs.  
    * Generates `dataset-metadata.json`.  
4.  **Deployment:** Uses the Kaggle API to push a new version of the dataset.

---

## ⚙️ Installation & Local Usage

If you want to run this script on your own machine:

### 1. Clone the Repo  
```bash  
git clone https://github.com/YOUR_USERNAME/YOUR_REPO_NAME.git  
cd YOUR_REPO_NAME
```

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

### 3. Run the Script
```bash
python main.py
```

*The script will create a folder named crypto_data and populate it with 100 CSV files.*

---

## 🤖 Automating Your Own Copy

If you fork this repository, you must set up your Kaggle credentials to enable the auto-upload feature.

1. Go to your **Kaggle Account Settings** and click **"Create New Token"** to download kaggle.json.  
2. In your GitHub Repository, go to **Settings > Secrets and variables > Actions**.  
3. Create two repository secrets:  
   * KAGGLE_USERNAME: Your Kaggle username (from the json file).  
   * KAGGLE_KEY: Your API key (from the json file).

Once set, the GitHub Action will automatically pick up these credentials and upload data to your account.

---

## 📜 License

This project is dedicated to the public domain under the The Unlicense.  
You are free to use, modify, distribute, and sell this software and its data without any restrictions or attribution requirements.  
For more information, please refer to [http://unlicense.org/](http://unlicense.org/).