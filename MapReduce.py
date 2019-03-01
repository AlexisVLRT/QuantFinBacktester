import pickle
import requests
import os
from threading import Thread
import time


class Mapper:
    def __init__(self, tasks, slaves_count, host, port):
        self.swarm_host = host
        self.swarm_port = port
        self.tasks = tasks
        self.reducer = Reducer(self.swarm_host, self.swarm_port)
        self.slave_count = slaves_count
        self.tasks_in_progress = 0
        self.run()

        while self.tasks_in_progress:
            time.sleep(0.1)
        print(self.reducer.raw_results)
        with open('results', 'w') as f:
            f.writelines(str(self.reducer.raw_results))

    def run(self):
        while len(tasks):
            if self.tasks_in_progress < self.slave_count:
                Thread(target=self.post_task, args=(tasks[0], )).start()
                self.tasks_in_progress += 1
                del tasks[0]
            time.sleep(0.1)

    def post_task(self, task_data):
        ticker = task_data
        print('Posting task')
        r = requests.post('http://{}:{}/upload'.format(self.swarm_host, self.swarm_port), files={'script': open('Strategy.py', 'rb'), 'data': open('Data/{}_5.pickle'.format(ticker), 'rb')})
        print(r.text)
        self.reducer.wait_for_result(ticker)
        self.tasks_in_progress -= 1


class Reducer:
    def __init__(self, host, port):
        self.swarm_host = host
        self.swarm_port = port
        self.raw_results = []

    def wait_for_result(self, task):
        done = False
        while not done:
            r = requests.get('http://{}:{}/result'.format(self.swarm_host, self.swarm_port))
            if 'response' in r.json().keys() and r.json()['response'] == 200:
                done = True
                self.raw_results = r.json()['data']
            else:
                time.sleep(0.1)

    def add_result(self, result):
        self.raw_results.append(result)


if __name__ == '__main__':
    filenames = os.listdir('Data')
    tasks = [name.split('_')[0] for name in filenames][:1]
    print(tasks)
    swarm_host, swarm_port = '149.91.83.188', 80
    # swarm_host, swarm_port = 'localhost', 9999
    Mapper(tasks, 1, swarm_host, swarm_port)
