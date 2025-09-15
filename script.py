import requests
import openai
import os
from dotenv import load_dotenv
import csv
load_dotenv()

POLYGON_API_KEY = os.getenv('POLYGON_API_KEY')
LIMIT = 1000
url = f'"https://api.polygon.io/v3/reference/tickers?market=stocks&active=true&order=asc&limit={LIMIT}&sort=ticker&apiKey={POLYGON_API_KEY}'

response = requests.get(url)

tickers = []
data = response.json()
for ticker in data['results']:
    tickers.append(ticker)


while 'next_url' in data:
    response = requests.get(data['next_url'] + f'?&apiKey={POLYGON_API_KEY}')
    data = response.json()
    for ticker in data['results']:
        tickers.append(ticker)

field_names = list(example_ticker.keys())
output_csv = 'tickers.csv'
with open(output_csv, mode='w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=field_names)
    writer.writeheader()
    for t in tickers:
        row = {key: t.get(key, '') for key in field_names}
        writer.writerow(row)
print(f'Wrote {len(tickers)} rows to {output_csv}') 