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
        if result == 202:
            return {'response': 400}
        else:
            result = 202
            request.files.get('script').save('Strategy.py', overwrite=True)
            data = pickle.loads(request.files.get('data').file.read())
            Thread(target=run_strategy, args=(task, data)).start()
            return {'response': 202}

    except Exception as e:
        result = -1
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


@route('/ping', method='GET')
def ping():
    global result
    uid = hash('It really does not matter what we hash. It changes every time the API starts so..')
    if result == -1:
        return {'response': 200, 'id': uid}
    else:
        return {'response': 400}


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
