from bottle import route, run, request
from importlib import reload
import pickle
import traceback

import Strategy


@route('/upload', method='POST')
def ingest():
    try:
        request.files.get('script').save('Strategy.py', overwrite=True)
        data = pickle.loads(request.files.get('data').file.read())

        reload(Strategy)
        return Strategy.Strategy(data).run()
    except Exception as e:
        return {'error': traceback.format_exc()}


if __name__ == '__main__':
    port = 9999
    run(host='0.0.0.0', port=port)