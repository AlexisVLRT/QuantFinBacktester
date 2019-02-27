from bottle import route, run, request
import os
import pickle
from importlib import reload
import Strategy


@route('/upload', method='POST')
def ingest():
    upload = request.files.get('upload_file')
    name, ext = os.path.splitext(upload.filename)
    if ext != '.pickle':
        return 'File extension not allowed.'

    data = pickle.loads(upload.file.getvalue())
    reload(Strategy)
    strat = Strategy.Strategy(data)

    return strat.result


if __name__ == '__main__':
    port = 9999
    run(host='localhost', port=port)