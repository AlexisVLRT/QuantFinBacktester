from bottle import route, run, request
from importlib import reload
import pickle

import Strategy


@route('/upload', method='POST')
def ingest():
    script = request.files.get('script').file.getvalue()
    data = pickle.loads(request.files.get('data').file.getvalue())
    with open('Strategy.py', 'wb') as f:
        f.write(script)

    reload(Strategy)
    return Strategy.Strategy(data).run()


if __name__ == '__main__':
    port = 9999
    run(host='localhost', port=port)