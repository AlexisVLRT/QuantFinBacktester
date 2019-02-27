import time


class Strategy:
    def __init__(self, data):
        self.data = data

    def run(self):
        print('Running strategy with data : ', self.data)
        start = time.time()
        while time.time() - start < 10:
            continue
        return {'Strategy 3': self.data}
