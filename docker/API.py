from bottle import route, run, request
from importlib import reload
import pickle
import traceback
from threading import Thread

import Strategy


@route('/upload/<task>', method='POST')
def ingest(task='task'):
    global result
    try:
        request.files.get('script').save('Strategy.py', overwrite=True)
        data = pickle.loads(request.files.get('data').file.read())

        if result == 202:
            return {'response': 400}
        else:
            result = 202
            Thread(target=run_strategy, args=(task, data)).start()
            return {'response': 202}

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


def run_strategy(task, data):
    global result
    try:
        reload(Strategy)
        result = {task: Strategy.Strategy(data).run()}

    except Exception as e:
        result = {'error': traceback.format_exc()}


if __name__ == '__main__':
    result = -1
    port = 9999
    run(host='0.0.0.0', port=port)
    # run(host='localhost', port=port)
