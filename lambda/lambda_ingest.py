import os
import json
import requests
import boto3

DYNAMODB_TABLE = os.environ.get('DYNAMODB_TABLE')
API_KEY = os.environ.get('ALPHAVANTAGE_API_KEY')
STOCK_SYMBOL = "AAPL"  # Adjust as needed

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(DYNAMODB_TABLE)

def handler(event, context):
    url = (
        f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY'
        f'&symbol={STOCK_SYMBOL}&interval=5min&apikey={API_KEY}'
    )
    response = requests.get(url)
    if response.status_code != 200:
        print(f"Error fetching data: {response.text}")
        return {'statusCode': 500, 'body': json.dumps('Error fetching data')}

    data = response.json()
    time_series = data.get("Time Series (5min)", {})
    for timestamp, values in time_series.items():
        item = {
            'symbol': STOCK_SYMBOL,
            'timestamp': timestamp,
            'open': values.get("1. open"),
            'high': values.get("2. high"),
            'low': values.get("3. low"),
            'close': values.get("4. close"),
            'volume': values.get("5. volume")
        }
        table.put_item(Item=item)
    return {'statusCode': 200, 'body': json.dumps('Data ingested successfully')}
