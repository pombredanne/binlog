from collections import namedtuple

from .abstract import IterSeek, Direction


class CursorProxy(IterSeek):

    def __init__(self, db, res, cursor, db_name, direction=Direction.F):
        self.db = db
        self.res = res
        self.cursor = cursor
        self.db_name = db_name
        self.direction = direction
        self.can_set = None

        self._dupkey = None
        self.seeked = None

        self.dupsort = res.db[db_name].flags(res.txn)['dupsort']

    def __next__(self):
        if self.dupsort and self.dupkey is None:
            raise RuntimeError("dupkey must be set before iteration")

        seeked = self.seeked
        if seeked is None:
            if self.direction is Direction.F:
                if self.dupsort:
                    found = self.cursor.first_dup()
                else:
                    found = self.cursor.first()
            else:
                if self.dupsort:
                    found = self.cursor.last_dup()
                else:
                    found = self.cursor.last()
            self.seeked = False
        elif seeked is False:
            if self.direction is Direction.F:
                found = self.cursor.next()
            else:
                found = self.cursor.prev()
        else:
            if self.can_set is False:
                raise StopIteration

            found = None
            self.seeked = False

        if found is False:
            raise StopIteration
        else:
            raw_key, raw_value = self.cursor.item()
            if raw_value == b'':
                raise StopIteration
            elif raw_key == b'':
                if self.dupsort:
                    # I think this is a bug in pylmdb
                    key = self._dupkey
                else:  # pragma: no branch
                    raise RuntimeError("key error")
            else:
                key = self._from_key(raw_key)

            if self.dupsort:
                # We compare key to seeked_key to find if we are out
                if key != self._dupkey:
                    raise StopIteration
                else:
                    return self._from_value(raw_value)
            else:
                # No need to compare keys
                return (key, self._from_value(raw_value))


    @property
    def dupkey(self):
        return self._dupkey

    @dupkey.setter
    def dupkey(self, value):
        self._dupkey = value
        if not self.cursor.set_key(self._to_key(value)):
            raise ValueError("key not found")

    def seek(self, key):
        self.seeked = key
        if self.dupkey is None:
            self.can_set = self.cursor.set_key(self._to_key(key))
            if not self.can_set:
                self.can_set = self.cursor.set_range(self._to_key(key))
                if self.direction == Direction.B:
                    if not self.can_set:
                        self.can_set = self.cursor.last()
                    else:
                        self.cursor.prev()
        else:
            self.can_set = self.cursor.set_key_dup(self._to_key(self.dupkey),
                                                   self._to_value(key))
            if not self.can_set:
                self.can_set = self.cursor.set_range_dup(
                    self._to_key(self.dupkey),
                    self._to_value(key))
                if self.direction == Direction.B:
                    if not self.can_set:
                        self.can_set = self.cursor.last_dup()
                    else:
                        self.cursor.prev_dup()

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
