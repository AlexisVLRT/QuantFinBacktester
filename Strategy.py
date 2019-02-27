import time


class Strategy:
    def __init__(self, data):
        self.data = data
        self.result = self.run()

    def run(self):
        print('Running strategy with data : ', self.data)
        start = time.time()
        while time.time() - start < 20:
            continue
        return {'Strategy 1': self.data}
