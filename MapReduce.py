import requests
import os
from threading import Thread
import time


class Mapper:
    def __init__(self, tasks, host, port):
        self.swarm_host = host
        self.swarm_port = port
        self.tasks = tasks
        self.reducer = Reducer(self.swarm_host, self.swarm_port)
        self.ack_flag = 0
        self.run()

        while self.reducer.tasks_in_progress:
            time.sleep(0.1)
        print(self.reducer.raw_results)

    def run(self):
        while len(tasks):
            if len(self.reducer.nodes):
                self.ack_flag = 0
                Thread(target=self.post_task, args=(tasks[0], )).start()
                while not self.ack_flag:
                    time.sleep(0.1)
                print('Tasks running :', self.reducer.tasks_in_progress)
                print('Tasks queued :', len(tasks))
                time.sleep(0.5)
            time.sleep(0.1)

    def post_task(self, task_data):
        ticker = task_data
        print('Posting task :', task_data)
        r = requests.post('http://{}:{}/upload/{}'.format(self.swarm_host, self.swarm_port, task_data), files={'script': open('Strategy.py', 'rb'), 'data': open('Data/{}_5.pickle'.format(ticker), 'rb')})
        if 'response' in r.json().keys():
            if r.json()['response'] == 202:
                print('Ok')
                del tasks[0]
                self.ack_flag = 1
                self.reducer.tasks_in_progress = self.reducer.nodes_count - len(self.reducer.nodes) + 1
                self.reducer.nodes.remove(r.json()['id'])
            elif r.json()['response'] == 400:
                print('Failed')
                self.ack_flag = -1

        if not self.reducer.is_running:
            Thread(target=self.reducer.wait_for_result).start()


class Reducer:
    def __init__(self, host, port):
        self.swarm_host = host
        self.swarm_port = port
        self.raw_results = {}
        self.is_running = False
        self.tasks_in_progress = 0
        self.nodes = self.get_nodes()
        self.nodes_count = len(self.nodes)

    def wait_for_result(self):
        print('Reducer Started')
        self.is_running = True
        done = False
        while not done:
            r = requests.get('http://{}:{}/result'.format(self.swarm_host, self.swarm_port))
            print(r, r.text)
            if 'response' in r.json().keys() and r.json()['response'] == 200:
                print('Received result', r.json())
                self.raw_results.update(r.json()['data'])
                self.nodes.add(r.json()['id'])
                self.tasks_in_progress = self.nodes_count - len(self.nodes) + 1
                print('Tasks running :', self.tasks_in_progress)
                with open('results', 'w') as f:
                    f.writelines(str(self.raw_results))
            else:
                time.sleep(0.1)

            if self.tasks_in_progress == 0:
                done = True
        print('Reducer Done')

    def get_nodes(self):
        ids = set([])
        i, supposed_n_nodes = 0, 2**16
        while i < supposed_n_nodes * 3:
            r = requests.get('http://{}:{}/ping'.format(self.swarm_host, self.swarm_port))
            node_id = r.json()['id']
            if node_id in ids:
                supposed_n_nodes = len(ids)
            ids.add(node_id)
            i += 1
        return ids


if __name__ == '__main__':
    filenames = os.listdir('Data')
    tasks = [name.split('_')[0] for name in filenames][:200]
    print(tasks)
    swarm_host, swarm_port = '149.91.83.188', 80
    # swarm_host, swarm_port = 'localhost', 9999
    Mapper(tasks=tasks, host=swarm_host, port=swarm_port)
