from collections import namedtuple


class CursorProxy(namedtuple('_CursorProxy', ['db', 'cursor'])):
    def get(self, key, default=None):
        raw = self.cursor.get(self.db.K.db_value(key), default=None)
        if raw is None:
            return default
        else:
            return self.db.V.python_value(raw)

    def put(self, key, value, **kwargs):
        return self.cursor.put(self.db.K.db_value(key),
                               self.db.V.db_value(value),
                               **kwargs)

    def putmulti(self, items, **kwargs):
        def _translate_items():
            for key, value in items:
                yield (self.db.K.db_value(key), self.db.V.db_value(value))

        return self.cursor.putmulti(_translate_items(), **kwargs)

    def pop(self, key):
        res = self.cursor.pop(self.db.K.db_value(key))
        if res is None:
            return None
        else:
            return self.db.V.python_value(res)

    def _iterate(self, iterator, keys, values):
        if keys and values:
            for raw_key, raw_value in iterator:
                yield (self.db.K.python_value(raw_key),
                       self.db.V.python_value(raw_value))
        elif keys:
            for raw_key in iterator:
                yield self.db.K.python_value(raw_key)
        elif values:
            for raw_key in iterator:
                yield self.db.V.python_value(raw_value)
        else:
            raise ValueError("keys and/or values must be true")

    def iternext(self, keys=True, values=True):
        return self._iterate(self.cursor.iternext(keys, values), keys, values)

    def iterprev(self, keys=True, values=True):
        return self._iterate(self.cursor.iterprev(keys, values), keys, values)
