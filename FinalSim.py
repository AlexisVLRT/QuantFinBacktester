import ast
import pandas as pd
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
cash, invested, total = initial_cash, initial_invested, initial_total
inv_fraction = 0.02
open_positions = {}
for timestamp, calls in ordered_calls.iterrows():
    for ticker, call in calls[0]:
        # print(timestamp, call)
        print(call, ticker)
        if call in ['buy', 'short'] and inv_fraction * initial_cash <= cash:
            open_positions[ticker] = stocks_data.loc[timestamp, ticker]
            invested += inv_fraction * initial_cash
            cash -= inv_fraction * initial_cash

        elif call in ['sell', 'cover'] and ticker in open_positions.keys():
            entry_price = open_positions[ticker]
            exit_price = stocks_data.loc[timestamp, ticker]
            if call == 'sell':
                profit = exit_price / entry_price - 1

            else:
                profit = 1 - exit_price / entry_price

            invested -= inv_fraction * initial_cash
            cash += (inv_fraction * initial_cash) * (profit + 1)
            total = invested + cash
        print(cash, invested, total)