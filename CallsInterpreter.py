import json
import pickle

import pymongo
from credentials import credentials
import ast
import pandas as pd
import matplotlib.pyplot as plt
import seaborn


def get_calls(event_id):
    client = pymongo.MongoClient(
            host=credentials['mongo']['host'],
            port=credentials['mongo']['port'],
            username=credentials['mongo']['username'],
            password=credentials['mongo']['password'],
    )
    records = client.qfbt[event_id].find({})
    out = []
    for record in records:
        out += ast.literal_eval(record['result'])
    df = pd.DataFrame(out)
    df = df.set_index(0)
    df = df.sort_index()
    df.columns = ['calls']
    return df


def load_stock_data(ticker):
    df = pickle.load(open('Data/{}_5.pickle'.format(ticker), 'rb'))
    return df


def plot(calls, stock_data):
    seaborn.set()
    fig, (ax1, ax2) = plt.subplots(2, 1, sharex='col')
    stock_data.plot(ax=ax1)
    colors = {'buy': 'blue', 'sell': 'cyan', 'short': 'red', 'cover': 'yellow'}
    print(calls)
    for call in calls.iterrows():
        timestamp, call_type = call[0], call[1][0]
        ax1.axvline(x=list(stock_data.index).index(timestamp), linewidth=0.5, linestyle='-', color=colors[call_type])
        ax2.axvline(x=list(stock_data.index).index(timestamp), linewidth=0.5, linestyle='-', color=colors[call_type])
    plt.show()


def parse_calls(calls):
    position = None
    valid_calls = []
    for call in calls.iterrows():
        timestamp, call_type = call[0], call[1][0]

        if call_type == 'buy' and position is None:
            position = 'long'
            valid_calls.append([timestamp, call_type])

        if call_type == 'sell' and position == 'long':
            position = None
            valid_calls.append([timestamp, call_type])

        if call_type == 'short' and position is None:
            position = 'short'
            valid_calls.append([timestamp, call_type])

        if call_type == 'cover' and position == 'short':
            position = None
            valid_calls.append([timestamp, call_type])

    df = pd.DataFrame(valid_calls).set_index(0)
    df.columns = ['calls']
    return df, valid_calls


def generate_results(calls_dict):
    with open('results', 'w') as f:
        json.dump(calls_dict, f)


if __name__ == '__main__':
    event_ids = ['1566918172-ADBE', '1566918386-CDNS', '1566918429-CENX', '1566918238-SYMC', '1566918260-TLRY', '1566918217-SWKS', '1566918319-TQQQ', '1566918365-CAR', '1566918144-AAPL', '1566918406-CELG', '1566918194-STX', '1566918297-TMUS', '1566918343-TRIP', '1566918472-CMCSA', '1566918275-TLT', '1566918450-CERN']
    calls_dict = {}
    for event_id in event_ids:
        ticker = event_id.split('-')[-1]
        calls = get_calls(event_id)
        valid_calls_df, valid_calls = parse_calls(calls)
        calls_dict[ticker] = valid_calls
        stock_data = load_stock_data(ticker)
        # plot(valid_calls, stock_data)
    generate_results(calls_dict)
