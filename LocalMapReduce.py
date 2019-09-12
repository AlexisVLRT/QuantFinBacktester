from multiprocessing import cpu_count, Pool
import os
from Strategy import Strategy
import pickle


def bootstrap_sim(ticker):
    data = pickle.load(open('Data/{}_5.pickle'.format(ticker), 'rb'))
    print(len(data))
    strat = Strategy(data)
    calls = strat.run()
    return calls


if __name__ == '__main__':
    # pool = Pool(2)
    # filenames = os.listdir('Data')
    # tasks = [name.split('_')[0] for name in filenames][:200]
    # results = pool.map(bootstrap_sim, tasks)
    # results_dict = dict(zip(tasks, results))
    # with open('results', 'w') as f:
    #     f.write(str(results_dict))
    bootstrap_sim('AAPL')

