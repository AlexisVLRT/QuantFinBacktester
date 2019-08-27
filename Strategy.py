import pickle
import pandas as pd


class Strategy:
    def __init__(self, data, start_offset, n_samples, buffer_offset_days=66, period=5):
        self.period = period
        self.day_length = 480 // period
        self.data = data
        self.start_offset = self.day_length * buffer_offset_days + start_offset  # About 3 months
        self.n_samples = n_samples
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
        for i in range(self.start_offset, self.start_offset + self.n_samples):
            if not i % 100:
                print(i, len(self.data))
            if self.start_offset > len(self.data):
                break
            self.current_timestamp = self.data.index[i]
            ema24 = self.data.loc[:self.current_timestamp].ewm(span=26 * self.day_length).mean()
            ema12 = self.data.loc[:self.current_timestamp].ewm(span=12 * self.day_length).mean()
            macd = ema12 - ema24
            signal = macd.ewm(span=9 * self.day_length).mean()
            diff = pd.Series(((macd - signal)/(macd - signal).std()).values[:, 0])
            diff2 = (diff.fillna(0).diff().ewm(span=0.5 * self.day_length).mean()).fillna(0) * 200

            # Buy when diff < 0 and diff2 goes < 0
            if diff.iloc[-1] < 0 and diff2.iloc[-1] < 0 < diff2.iloc[-2]:
                self.buy()

            # Sell when diff goes > 0
            if diff.iloc[-1] > 0 > diff.iloc[-2]:
                self.sell()

            # short when diff > 0 and diff2 goes > 0
            if diff.iloc[-1] > 0 and diff2.iloc[-1] > 0 > diff2.iloc[-2]:
                self.short()

            # Cover when diff goes < 0
            if diff.iloc[-1] < 0 < diff.iloc[-2]:
                self.cover()
        return self.calls


if __name__ == '__main__':
    ticker = 'STNE'
    data = pickle.load(open('Data/{}_5.pickle'.format(ticker), 'rb'))
    strat = Strategy(data, 500, 1000)
    calls = strat.run()
    print(calls)