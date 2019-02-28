from Strategy import Strategy
import pickle
import requests
import io
from threading import Thread
import time


class Mapper:
    def __init__(self, tasks, reducer, slaves_count, host, port):
        self.swarm_host = host
        self.swarm_port = port
        self.tasks = tasks
        self.reducer = reducer
        self.slave_count = slaves_count
        self.tasks_in_progress = 0
        self.run()

        while self.tasks_in_progress:
            time.sleep(0.1)
        print(self.reducer.raw_results)

    def run(self):
        while len(tasks):
            if self.tasks_in_progress < self.slave_count:
                Thread(target=self.post_task, args=(tasks[0], )).start()
                self.tasks_in_progress += 1
                del tasks[0]
            time.sleep(0.1)

    def post_task(self, task_data):
        print('Posting task')
        data = pickle.dumps(task_data)
        r = requests.post('http://{}:{}/upload'.format(self.swarm_host, self.swarm_port), files={'script': open('Strategy.py', 'rb'), 'data': io.BytesIO(data)})
        print(r.text)
        self.reducer.add_result(r.json())
        self.tasks_in_progress -= 1


class Reducer:
    def __init__(self):
        self.raw_results = []

    def add_result(self, result):
        self.raw_results.append(result)


if __name__ == '__main__':
    tasks = ['A first task', 'Another task']
    swarm_host, swarm_port = 'localhost', 9999
    Mapper(tasks, Reducer(), 1, swarm_host, swarm_port)