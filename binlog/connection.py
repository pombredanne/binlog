from collections import namedtuple


class Connection(namedtuple('_Connection', ('model', 'path', 'kwargs'))):
    def __init__(self, *args, **kwargs):
        super().__init__()
        self.closed = False

    def __enter__(self):
        return self

    def __exit__(self, *_):
        pass

    def close(self):
        self.closed = True
