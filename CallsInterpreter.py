import json
import pickle

import pymongo
from credentials import credentials
import ast
import pandas as pd
import matplotlib.pyplot as plt
import seaborn


def get_calls(sim_id, ticker):
    client = pymongo.MongoClient(
            host=credentials['mongo']['host'],
            port=credentials['mongo']['port'],
            username=credentials['mongo']['username'],
            password=credentials['mongo']['password'],
    )
    records = client.qfbt[sim_id].find({'ticker': ticker})
    out = []
    for record in records:
        out += record['results']
    df = pd.DataFrame(out)
    df = df.set_index(0)
    df = df.sort_index()
    df.columns = ['calls']
    return df


def get_tickers(sim_id):
    client = pymongo.MongoClient(
        host=credentials['mongo']['host'],
        port=credentials['mongo']['port'],
        username=credentials['mongo']['username'],
        password=credentials['mongo']['password'],
    )
    records = client.qfbt[sim_id].find({})
    out = set([])
    for record in records:
        out.add(record['ticker'])
    return list(out)


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

    df = pd.DataFrame(valid_calls)
    if len(valid_calls):
        df = df.set_index(0)
        df.columns = ['calls']
    return df, valid_calls


def generate_results(calls_dict):
    with open('results', 'w') as f:
        json.dump(calls_dict, f)


if __name__ == '__main__':
    sim_id = '1566984985'
    tickers = get_tickers(sim_id)

    calls_dict = {}
    for ticker in tickers:
        calls = get_calls(sim_id, ticker)
        valid_calls_df, valid_calls = parse_calls(calls)
        calls_dict[ticker] = valid_calls
        stock_data = load_stock_data(ticker)
        # plot(valid_calls, stock_data)
    generate_results(calls_dict)
