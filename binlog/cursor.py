from collections import namedtuple


class CursorProxy(namedtuple('_CursorProxy', ['db', 'res', 'cursor', 'db_name'])):

    def _to_key(self, data):
        return self.db.K.db_value(data)

    def _from_key(self, data):
        return self.db.K.python_value(data)

    def _to_value(self, data):
        return self.db.V.db_value(data)

    def _from_value(self, data):
        return self.db.V.python_value(data)

    def get(self, key, default=None):
        raw = self.cursor.get(self._to_key(key), default=None)
        if raw is None:
            return default
        else:
            return self._from_value(raw)

    def put(self, key, value, **kwargs):
        return self.cursor.put(self._to_key(key),
                               self._to_value(value),
                               **kwargs)

    def putmulti(self, items, **kwargs):
        def _translate_items():
            for key, value in items:
                yield (self._to_key(key), self._to_value(value))

        return self.cursor.putmulti(_translate_items(), **kwargs)

    def pop(self, key):
        res = self.cursor.pop(self._to_key(key))
        if res is None:
            return None
        else:
            return self._from_value(res)

    def _iterate(self, iterator, keys, values):
        if keys and values:
            for raw_key, raw_value in iterator:
                yield (self._from_key(raw_key),
                       self._from_value(raw_value))
        elif keys:
            for raw_key in iterator:
                yield self._from_key(raw_key)
        elif values:
            for raw_key in iterator:
                yield self._from_value(raw_value)
        else:
            raise ValueError("keys and/or values must be true")

    def iternext(self, keys=True, values=True):
        return self._iterate(self.cursor.iternext(keys, values), keys, values)

    def iterprev(self, keys=True, values=True):
        return self._iterate(self.cursor.iterprev(keys, values), keys, values)

    def delete(self, key, value=None):
        db_handler = self.db.get_db_handler(self.res, self.db_name)
        if value is not None:
            return self.res.txn.delete(self._to_key(key),
                                       value=self._to_value(value),
                                       db=db_handler)
        else:
            return self.res.txn.delete(self._to_key(key), db=db_handler)

    def set_range(self, key):
        return self.cursor.set_range(self._to_key(key))
