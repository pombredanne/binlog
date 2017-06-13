from collections import namedtuple


OpenConnection = namedtuple('OpenConnection', ['connection', 'params'])


class ConnectionManager:
    def __init__(self):
        self.connections = dict()

    def open(self, model, path, connection_class, kwargs):
        if path not in self.connections:
            self.connections[path] = OpenConnection(
                connection_class(model=model, path=path, kwargs=kwargs),
                frozenset(kwargs.items()))
        elif frozenset(kwargs.items()) != self.connections[path].params:
            raise ValueError(
                "Cannot open twice a database with different params")
        else:
            pass

        return self.connections[path].connection.open()

    def close(self, path):
        if path in self.connections:
            del self.connections[path]
            return True
        else:
            raise ValueError("Connection not found")


PROCESS_CONNECTIONS = ConnectionManager()
