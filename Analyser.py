import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import pickle
from sklearn.preprocessing import StandardScaler
import seaborn;seaborn.set()


def analyse(ticker, period, calls):
    data = pickle.load(open('Data/{}_{}.pickle'.format(ticker, period), 'rb'))

    # calls = {'201807191515': 'cover', '201811010925': 'sell', '201810021505': 'cover', '201811121055': 'cover', '201805311330': 'sell', '201803270900': 'buy', '201804041255': 'buy', '201811261010': 'sell', '201806071335': 'short', '201808021515': 'sell', '201808021210': 'buy', '201807261250': 'cover', '201805031035': 'cover', '201805021250': 'short', '201810150905': 'sell', '201807190940': 'short', '201810021400': 'short', '201810261520': 'sell', '201809051105': 'short', '201805011315': 'short', '201804231415': 'short', '201807120930': 'sell', '201807021430': 'buy', '201808151025': 'cover', '201806281555': 'buy', '201808290900': 'sell', '201809201125': 'buy', '201806200950': 'cover', '201804041530': 'sell', '201807260935': 'short', '201806211100': 'short', '201808151005': 'short', '201809171050': 'sell', '201806250900': 'cover', '201805231510': 'buy', '201804021035': 'sell', '201809271000': 'sell', '201805011550': 'cover', '201810311045': 'buy', '201811121020': 'short', '201805171130': 'cover', '201809061010': 'cover', '201806290940': 'sell', '201804241020': 'cover', '201809120905': 'buy', '201805071525': 'short', '201810121540': 'buy', '201811260925': 'buy', '201808221510': 'buy', '201810231450': 'buy'}
    sorted_calls = pd.DataFrame.from_dict(calls, orient='index').sort_index()
    sorted_calls = sorted_calls[:len(sorted_calls) // 2 * 2]

    recap = []
    for thing in sorted_calls.iterrows():
        recap.append(thing[0])
        if thing[1][0] == 'sell':
            recap.append('long')
        elif thing[1][0] == 'cover':
            recap.append('short')

    recap = pd.DataFrame(np.array(recap).reshape((-1, 3)), columns=['EntryDate', 'ExitDate', 'Position'])

    recap['EntryPrice'] = data.loc[recap['EntryDate']].values
    recap['ExitPrice'] = data.loc[recap['ExitDate']].values
    recap['Profit'] = recap[recap['Position'] == 'long'].loc[:, 'ExitPrice'] / recap[recap['Position'] == 'long'].loc[:, 'EntryPrice'] - 1
    recap['Profit2'] = 1 - recap[recap['Position'] == 'short'].loc[:, 'ExitPrice'] / recap[recap['Position'] == 'short'].loc[:, 'EntryPrice']
    recap['Profit'] = recap['Profit'].fillna(0) + recap['Profit2'].fillna(0)
    recap = recap.drop(columns=['Profit2'])

    profits = recap['Profit']
    print('N trades :', len(profits))
    print('N Wins :', len(profits[profits > 0]))
    print('N Lost :', len(profits[profits < 0]))
    print('% win :', 100 * len(profits[profits > 0])/len(profits))
    print('% lost :', 100 * len(profits[profits <= 0])/len(profits))
    print('avg win profit :', profits[profits > 0].mean())
    print('avg loss profit :', profits[profits < 0].mean())
    print('Average profit per trade:', profits.mean())
    print('Median profit per trade:', profits.median())
    print('Profit std :', profits.std())
    print('Overall profit over test period :', sum(profits) * 1)

    ema24 = data.loc[:].ewm(span=26 * 96).mean()
    ema12 = data.loc[:].ewm(span=12 * 96).mean()
    macd = ema12 - ema24
    signal = macd.ewm(span=9 * 96).mean()
    diff = (macd - signal)
    diff = pd.Series(StandardScaler(with_mean=False).fit_transform(diff.values.reshape(-1, 1)).flatten())
    diff2 = (diff.fillna(0).diff().ewm(span=0.5 * 96).mean()).fillna(0) * 200

    fig, (ax, ax2) = plt.subplots(2, 1, sharex='col')
    for timestamp, call in calls.items():
        colors = {'buy': 'blue', 'sell': 'cyan', 'short': 'red', 'cover': 'yellow'}
        # ax.axvline(x=list(data.index).index(timestamp), linewidth=0.5, linestyle='-', color=colors[call])
        # ax2.axvline(x=list(data.index).index(timestamp), linewidth=0.5, linestyle='-', color=colors[call])
    data.reset_index(drop=True).plot(ax=ax)

    ax2.fill_between(range(len(data)), diff2, alpha=0.5)
    ax2.fill_between(range(len(data)), diff, alpha=0.5)
    plt.show()