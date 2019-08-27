import json
import os
import pickle

import pymongo
import time
import shutil

import boto3
import csv

from threading import Thread
from credentials import credentials as mongo_credentials


def execute(ticker, start, span, event_id, boto_client):
    payload = {
        'ticker': ticker,
        'start': start,
        'span': span,
        'id': event_id
    }
    boto_client.invoke(
        # ClientContext='MyApp',
        FunctionName='QuantFinBT',
        InvocationType='Event',
        LogType='Tail',
        Payload=json.dumps(payload).encode(),
    )


def get_boto_client():
    with open('credentials.csv', 'r') as csvFile:
        reader = csv.reader(csvFile)
        lines = []
        for row in reader:
            lines.append(row)
        credentials = dict(zip(lines[0], lines[1]))

    boto_client = boto3.client(
        'lambda',
        aws_access_key_id=credentials['Access key ID'],
        aws_secret_access_key=credentials['Secret access key'],
        region_name='eu-west-3'
    )
    return boto_client


def update_function():
    print('Creating archive...')
    shutil.copy(os.path.abspath(os.path.join(os.path.dirname(__file__), 'Strategy.py')),
                os.path.abspath(os.path.join(os.path.dirname(__file__), 'lambda_distrib')))
    shutil.copy(os.path.abspath(os.path.join(os.path.dirname(__file__), 'lambda_function.py')),
                os.path.abspath(os.path.join(os.path.dirname(__file__), 'lambda_distrib')))

    shutil.make_archive('lambda_distrib', 'zip', 'lambda_distrib/')

    with open('lambda_distrib.zip', 'rb') as f:
        zip_bytes = f.read()

    print('Pushing...')
    response = get_boto_client().update_function_code(
        FunctionName='QuantFinBT',
        ZipFile=zip_bytes,
        Publish=True,
    )


if __name__ == '__main__':
    update_function()

    tickers = ['AAPL', 'ADBE', 'STNE', 'STX', 'SWKS', 'SYMC', 'TLRY', 'TLT', 'TMUS', 'TQQQ', 'TRIP', 'BPR', 'CAR', 'CDNS', 'CELG', 'CENX', 'CERN', 'CMCSA']
    ids = set([])
    for ticker in tickers:
        print(f'{tickers.index(ticker) + 1}/{len(tickers)}')
        event_id = str(int(time.time())) + '-' + ticker
        print(event_id)

        print('Mapping tasks...')
        data = pickle.load(open('Data/{}_5.pickle'.format(ticker), 'rb'))

        data_size = len(data) - 6336  # 6336 is the number of samples for 3 months
        n_chunks = data_size // 100

        print(f'From sample 0 to {n_chunks * 100}')
        for i in range(0, n_chunks * 100, 100):
            Thread(target=execute, args=(ticker, i, 100, event_id, get_boto_client())).start()

        client = pymongo.MongoClient(
            host=mongo_credentials['mongo']['host'],
            port=mongo_credentials['mongo']['port'],
            username=mongo_credentials['mongo']['username'],
            password=mongo_credentials['mongo']['password'],
        )

        print('Awaiting results...')
        n_docs = 0
        while n_docs != n_chunks and n_chunks > 0:
            ids.add(event_id)
            cursor = client.qfbt[event_id].find({})
            n_docs = 0
            for document in cursor:
                n_docs += 1
            print(n_docs)
            time.sleep(1)
        print('Done!')
    print('Fully Finished!')
    print(list(ids))
