import os
import json
import boto3
from boto3.dynamodb.conditions import Key

DYNAMODB_TABLE = os.environ.get('DYNAMODB_TABLE')
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(DYNAMODB_TABLE)

def handler(event, context):
    # For this example, we return the 10 latest records for AAPL.
    symbol = "AAPL"
    response = table.query(
        KeyConditionExpression=Key('symbol').eq(symbol),
        ScanIndexForward=False,  # Sort descending by timestamp
        Limit=10
    )
    items = response.get('Items', [])
    return {
        'statusCode': 200,
        'headers': {'Content-Type': 'application/json'},
        'body': json.dumps(items)
    }
