from Strategy import Strategy
import pickle
import requests
import io
from threading import Thread
import time


class Mapper:
    def __init__(self, tasks, reducer, slaves_count):
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

    def post_task(self, task):
        print('Posting task')
        bundle = pickle.dumps(Strategy(task))
        r = requests.post('http://149.91.83.188:80/upload', files={'upload_file': io.BytesIO(bundle)})
        self.reducer.add_result(r.json())
        self.tasks_in_progress -= 1


class Reducer:
    def __init__(self):
        self.raw_results = []

    def add_result(self, result):
        self.raw_results.append(result)


if __name__ == '__main__':
    tasks = ['A task', 'Another task']
    Mapper(tasks, Reducer(), 1)