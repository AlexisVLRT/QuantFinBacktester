from bottle import route, run, request
import pickle


@route('/upload', method='POST')
def ingest():
    upload = request.files.get('upload_file')
    bundle = pickle.loads(upload.file.getvalue())
    return bundle.run()


if __name__ == '__main__':
    port = 9999
    run(host='0.0.0.0', port=port)