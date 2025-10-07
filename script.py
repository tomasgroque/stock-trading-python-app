import requests
import os
from dotenv import load_dotenv
import csv
load_dotenv()
import snowflake.connector
from datetime import datetime

POLYGON_API_KEY = os.getenv('POLYGON_API_KEY')
LIMIT = 1000

def run_stock_job():
    DS = datetime.now().strftime('%Y-%m-%d')
    url = f"https://api.polygon.io/v3/reference/tickers?market=stocks&active=true&order=asc&limit={LIMIT}&sort=ticker&apiKey={POLYGON_API_KEY}"
    response = requests.get(url)
    tickers = []
    raw_data = response.json()
    data = raw_data['results']

    for ticker in data:
        tickers.append(ticker)

    while 'next_url' in data:
        response = requests.get(data['next_url'] + f'?&apiKey={POLYGON_API_KEY}')
        raw_data = response.json()
        data = raw_data['results']
        for ticker in data['results']:
            tickers.append(ticker)

    example_ticker = {
        'ticker': 'BAOS', 
        'name': 'Baosheng Media Group Holdings Limited Ordinary shares', 
        'market': 'stocks', 
        'locale': 'us', 
        'primary_exchange': 'XNAS',
        'type': 'CS', 
        'active': True, 
        'currency_name': 'usd', 
        'cik': '0001811216', 
        'last_updated_utc': '2025-09-15T06:04:58.614854517Z'
    }

    field_names = list(example_ticker.keys())
    output_csv = 'tickers.csv'
    with open(output_csv, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=field_names)
        writer.writeheader()
        for t in tickers:
            row = {key: t.get(key, '') for key in field_names}
            writer.writerow(row)
    print(f'Wrote {len(tickers)} rows to {output_csv}')

if __name__ == '__main__':
    run_stock_job()