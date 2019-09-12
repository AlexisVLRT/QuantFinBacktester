import json
import pickle
import os
import uuid

files = os.listdir('Data')

jobs = []
for file in files:
    with open('Data/' + file, 'rb') as f:
        data = pickle.load(f)

        data_size = len(data) - 6336  # 6336 is the number of samples for 3 months
        n_chunks = data_size // 100
        if data_size > 0:
            for i in range(0, n_chunks * 100, 100):
                # print(f'{file.split("_")[0]} from sample {i} to {i + 100}')
                job = {
                    'ticker': file.split("_")[0],
                    'start': i,
                    'span': 100,
                    'id': str(uuid.uuid4())
                }
                jobs.append(job)

with open('jobs', 'w') as f:
    json.dump(jobs, f)
