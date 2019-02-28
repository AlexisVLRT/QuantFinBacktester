from bottle import route, run, request
from importlib import reload
import pickle

import Strategy


@route('/upload', method='POST')
def ingest():
    request.files.get('script').save('Strategy.py', overwrite=True)
    data = pickle.loads(request.files.get('data').file.read())

    reload(Strategy)
    return Strategy.Strategy(data).run()


if __name__ == '__main__':
    port = 9999
    run(host='0.0.0.0', port=port)