import csv
import json
import os

import boto3

from Strategy import Strategy
import pickle
import pymongo
from credentials import credentials
import time


def bootstrap_sim(ticker, start, n_samples):
    raw_data = get_boto_client('s3').get_object(Bucket='stockdatalambda', Key=ticker + '_5.pickle')
    data = pickle.loads(raw_data['Body'].read())
    strat = Strategy(data, start_offset=start, n_samples=n_samples)
    calls = strat.run()
    return calls


def lambda_handler(event, context):
    results = bootstrap_sim(event['ticker'], event['start'], event['span'])
    report = json.loads(json.dumps(event))
    report['results'] = results

    client = pymongo.MongoClient(
        host=credentials['mongo']['host'],
        port=credentials['mongo']['port'],
        username=credentials['mongo']['username'],
        password=credentials['mongo']['password'],
    )
    client.qfbt[event['sim_id']].insert_one(json.loads(json.dumps(report)))
    return {
        'statusCode': 200,
        'body': json.dumps(report)
    }


def get_boto_client(client_type='lambda'):
    with open(os.path.dirname(__file__) + '/credentials.csv', 'r') as csvFile:
        reader = csv.reader(csvFile)
        lines = []
        for row in reader:
            lines.append(row)
        credentials = dict(zip(lines[0], lines[1]))

    boto_client = boto3.client(
        client_type,
        aws_access_key_id=credentials['Access key ID'],
        aws_secret_access_key=credentials['Secret access key'],
        region_name='eu-west-3'
    )
    return boto_client


if __name__ == '__main__':
    ticker = 'AAPL'
    event_id = str(int(time.time())) + '-' + ticker
    print(event_id)
    result = lambda_handler({'ticker': ticker, 'start': 0, 'span': 100, 'id': event_id, 'sim_id': 'test'}, '')
    print(result)

