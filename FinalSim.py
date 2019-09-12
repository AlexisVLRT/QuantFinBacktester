import ast
import copy
from random import randint

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn
import pickle

with open('results', 'r') as f:
    all_calls = ast.literal_eval(f.readline())

ordered_calls = {}
for ticker, calls in all_calls.items():
    for timestamp, position in calls:
        if timestamp in ordered_calls.keys():
            ordered_calls[timestamp][0].append((ticker, position))
        else:
            ordered_calls[timestamp] = [[(ticker, position)]]

ordered_calls = pd.DataFrame.from_dict(ordered_calls, orient='index').sort_index()

print('Loading stocks data...')
stocks_data = pd.DataFrame()
tickers = all_calls.keys()
for ticker in tickers:
    stocks_data = pd.concat([stocks_data, pickle.load(open('Data/{}_5.pickle'.format(ticker), 'rb'))['close']], axis=1, join='outer', sort=True)
stocks_data = stocks_data.fillna(method='ffill', inplace=False).fillna(method='bfill')
stocks_data.columns = tickers
print('Done')


initial_cash, initial_invested, initial_total = 1, 0, 1
inv_fraction = 0.3
risk_free_rate = 0.75
random_start_offsets = [randint(0, 2000) for i in range(1000)]
sharpe_ratios, returns = [], []
reports = []
for i in random_start_offsets:
    print(f'{random_start_offsets.index(i) + 1}/{len(random_start_offsets)}')
    cash, invested, total = initial_cash, initial_invested, initial_total
    open_positions = {}
    report = []
    for timestamp, calls in list(ordered_calls.iterrows())[i:i + 500]:
        for ticker, call in calls[0]:
            acted = False
            if call in ['buy', 'short'] and cash >= inv_fraction * max(initial_cash, cash):
                open_positions[ticker] = {
                    'entry_price': stocks_data.loc[timestamp, ticker],
                    'invested': inv_fraction * max(initial_cash, cash)
                }

                invested += inv_fraction * max(initial_cash, cash)
                cash -= inv_fraction * max(initial_cash, cash)
                acted = True

            elif call in ['sell', 'cover'] and ticker in open_positions.keys():
                entry_price = open_positions[ticker]['entry_price']
                exit_price = stocks_data.loc[timestamp, ticker]

                if call == 'sell':
                    profit = exit_price / entry_price - 1
                else:
                    profit = 1 - exit_price / entry_price

                investment_value = open_positions[ticker]['invested'] * (profit + 1)

                invested -= open_positions[ticker]['invested']
                cash += open_positions[ticker]['invested'] * (profit + 1)
                total = invested + cash
                del open_positions[ticker]
                acted = True

            # if acted:
            #     print(call, ticker)
            #     print(f'Cash: {cash}\nInvested: {invested}\nTotal: {total}\n\n')
        report.append((cash, invested, total))

    report = pd.DataFrame(report)
    if len(report):
        report.columns = ['cash', 'invested', 'total']

        report['excess_return'] = report['total'] - risk_free_rate
        sharpe_ratio = (total - risk_free_rate) / report['excess_return'].std()
        sharpe_ratios.append(sharpe_ratio)
        returns.append(total)
        reports.append(copy.deepcopy(report))

print(sharpe_ratios)
print('Average Sharpe ratio', sum(sharpe_ratios) / len(sharpe_ratios))
print('Average returns', sum(pd.Series(returns) - 1) / len(returns))

seaborn.set()
fig, (ax1, ax2) = plt.subplots(2, 1)
max_length = max([len(report) for report in reports])
for report in reports:
    padded = pd.concat([pd.Series([1] * (max_length - len(report))), report['total']], ignore_index=True)
    padded.plot(ax=ax1, legend=False)
pd.Series(returns).hist(bins=250)
plt.show()
