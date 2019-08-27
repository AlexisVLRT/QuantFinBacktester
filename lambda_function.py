import json
from Strategy import Strategy
import pickle
import pymongo
from credentials import credentials
import time


def bootstrap_sim(ticker, start, n_samples):
    data = pickle.load(open('Data/{}_5.pickle'.format(ticker), 'rb'))
    strat = Strategy(data, start_offset=start, n_samples=n_samples)
    calls = strat.run()
    return calls


def lambda_handler(event, context):
    results = bootstrap_sim(event['ticker'], event['start'], event['span'])
    client = pymongo.MongoClient(
        host=credentials['mongo']['host'],
        port=credentials['mongo']['port'],
        username=credentials['mongo']['username'],
        password=credentials['mongo']['password'],
    )
    client.qfbt[event['id']].insert_one({'result': str(results)})
    return {
        'statusCode': 200,
        'body': json.dumps({
            'result': results
        })
    }


if __name__ == '__main__':
    # pool = Pool(2)
    # filenames = os.listdir('Data')
    # tasks = [name.split('_')[0] for name in filenames][:2]
    # results = pool.map(bootstrap_sim, tasks)
    # results_dict = dict(zip(tasks, results))
    # with open('results', 'w') as f:
    #     f.write(str(results_dict))

    ticker = 'AAPL'
    event_id = str(int(time.time())) + '-' + ticker
    print(event_id)
    result = lambda_handler({'ticker': ticker, 'start': 0, 'span': 100, 'id': event_id}, '')
    print(result)
