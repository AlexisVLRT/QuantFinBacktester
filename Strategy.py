import pickle
import pandas as pd


class Strategy:
    def __init__(self, data, period=5):
        self.period = period
        self.day_length = 480 // period
        self.data = data
        self.start_offset = self.day_length * 66  # About 3 months
        self.current_timestamp = None
        self.position = 0
        self.calls = []

    def buy(self):
        print('Buying')
        self.position = (1, self.current_timestamp)
        self.calls.append((self.current_timestamp, 'buy'))

    def sell(self):
        print('Selling')
        self.position = 0
        self.calls.append((self.current_timestamp, 'sell'))

    def short(self):
        print('Shorting')
        self.position = (-1, self.current_timestamp)
        self.calls.append((self.current_timestamp, 'short'))

    def cover(self):
        print('Covering')
        self.position = 0
        self.calls.append((self.current_timestamp, 'cover'))

    def run(self):
        for i in range(self.start_offset, len(self.data)):
            if not i % 100:
                print(i, len(self.data))
            self.current_timestamp = self.data.index[i]
            ema24 = self.data.loc[:self.current_timestamp].ewm(span=26 * self.day_length).mean()
            ema12 = self.data.loc[:self.current_timestamp].ewm(span=12 * self.day_length).mean()
            macd = ema12 - ema24
            signal = macd.ewm(span=9 * self.day_length).mean()
            diff = pd.Series(((macd - signal)/(macd - signal).std()).values[:, 0])
            diff2 = (diff.fillna(0).diff().ewm(span=0.5 * self.day_length).mean()).fillna(0) * 200

            # Buy when diff < 0 and diff2 goes > 0
            if self.position == 0:
                if diff.iloc[-1] < 0 and diff2.iloc[-1] > 0 > diff2.iloc[-2]:
                    self.buy()

            # short when diff > 0 and diff2 goes < 0
            if self.position == 0:
                if diff.iloc[-1] > 0 and diff2.iloc[-1] < 0 < diff2.iloc[-2]:
                    self.short()

            # Sell as soon as we achieve a sufficient year-based profit or after three days if we lose too much
            if type(self.position) is tuple and self.position[0] == 1:
                open_date = self.position[1]
                price_in = self.data.loc[open_date, 'close']
                y_profit = 1
                n_ticks = i - list(self.data.index).index(open_date)

                thresh_profit = y_profit * self.period * price_in / (480 * 252) * n_ticks + price_in + price_in * 0.01
                thresh_loss = -y_profit * self.period * price_in / (480 * 252) * n_ticks + price_in - price_in * 0.01

                if self.data.iloc[i, 0] < thresh_loss or diff.iloc[-2] < 0 < diff.iloc[-1] or self.data.iloc[i, 0] > thresh_profit:
                    self.sell()

            # Cover as soon as we achieve a sufficient year-based profit or after three days if we lose too much
            if type(self.position) is tuple and self.position[0] == -1:
                open_date = self.position[1]
                price_in = self.data.loc[open_date, 'close']
                y_profit = 1
                n_ticks = i - list(self.data.index).index(open_date)

                thresh_profit = -y_profit * self.period * price_in / (480 * 252) * n_ticks + price_in - price_in * 0.01
                thresh_loss = y_profit * self.period * price_in / (480 * 252) * n_ticks + price_in + price_in * 0.01

                if self.data.iloc[i, 0] > thresh_loss or diff.iloc[-2] > 0 > diff.iloc[-1] or self.data.iloc[i, 0] < thresh_profit:
                    self.cover()
        return self.calls


if __name__ == '__main__':
    ticker = 'ADBE'
    data = pickle.load(open('Data/{}_5.pickle'.format(ticker), 'rb'))
    strat = Strategy(data)
    calls = strat.run()
    print(calls)