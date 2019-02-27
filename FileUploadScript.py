import requests

r = requests.post('http://149.91.83.188.:80/upload', files={'upload_file': open('helloworld.pickle','rb')})
print(r.text)