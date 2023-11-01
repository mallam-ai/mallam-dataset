import json


class State:
    def __init__(self, filename='state.json'):
        self.filename = filename
        self.data = {}
        try:
            with open(filename) as f:
                self.data = json.load(f)
        except FileNotFoundError:
            pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        with open(self.filename, 'w') as f:
            json.dump(self.data, f, indent=2)
