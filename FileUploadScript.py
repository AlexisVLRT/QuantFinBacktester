import requests
import io
import pickle

r = requests.post('http://localhost:9999/upload', files={'upload_file': io.BytesIO(pickle.dumps('lel'))})
print(r.text)