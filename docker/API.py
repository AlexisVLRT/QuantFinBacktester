from bottle import route, run, request
from importlib import reload
import pickle
import traceback
from threading import Thread

import Strategy


@route('/upload', method='POST')
def ingest():
    global result
    try:
        request.files.get('script').save('Strategy.py', overwrite=True)
        data = pickle.loads(request.files.get('data').file.read())

        reload(Strategy)
        Thread(target=run_strategy, args=(data, )).start()
        result = 202

    except Exception as e:
        return {'error': traceback.format_exc()}


@route('/result', method='GET')
def get_result():
    global result
    if result == 202:
        return {'response': 202}
    elif result == -1:
        return {'response': 400}
    else:
        ret_val = result
        result = -1
        return {'response': 200, 'data': ret_val}


def run_strategy(data):
    global result
    reload(Strategy)
    result = Strategy.Strategy(data).run()


if __name__ == '__main__':
    result = -1
    port = 9999
     # run(host='0.0.0.0', port=port)
    run(host='localhost', port=port)
