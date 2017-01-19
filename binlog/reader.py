from collections import namedtuple
from functools import wraps

import lmdb

from .serializer import NumericSerializer, ObjectSerializer


def mask_exception(exp, mask):
    def _mask_exception(f):
        @wraps(f)
        def wrapper(*args, **kwds):
            try:
                return f(*args, **kwds)
            except exp as exc:
                raise mask from exc
        return wrapper
    return _mask_exception


class Reader:
    def __init__(self, connection, name, config):
        self.connection = connection
        self.name = name
        self.config = config
        self.closed = False

    def close(self):
        self.closed = True

    def __enter__(self):
        return self

    def __exit__(self, *_, **__):
        self.close()

    def __iter__(self):
        def _iter():
            try:
                with self.connection._data(write=False) as res:
                    with res.txn.cursor(res.db['entries']) as cursor:
                        for raw_key, raw_value in cursor:
                            entry = self.connection.model(
                                **ObjectSerializer.python_value(raw_value))
                            entry.pk = NumericSerializer.python_value(raw_key)
                            entry.saved = True
                            yield entry
            except lmdb.ReadonlyError as exc:
                raise StopIteration from exc

        return _iter()

    def __reversed__(self):
        def _riter():
            try:
                with self.connection._data(write=False) as res:
                    with res.txn.cursor(res.db['entries']) as cursor:
                        for raw_key, raw_value in cursor.iterprev():
                            entry = self.connection.model(
                                **ObjectSerializer.python_value(raw_value))
                            entry.saved = True
                            entry.pk = NumericSerializer.python_value(raw_key)
                            yield entry
            except lmdb.ReadonlyError as exc:
                raise StopIteration from exc


        return _riter()

    @mask_exception(lmdb.ReadonlyError, IndexError)
    def __getitem__(self, key):
        with self.connection._data(write=False) as res:
            with res.txn.cursor(res.db['entries']) as cursor:
                if key < 0:
                    for idx, raw_item in enumerate(cursor.iterprev(), 1):
                        if key + idx == 0:
                            raw_key, raw_value = raw_item
                            entry = self.connection.model(
                                **ObjectSerializer.python_value(raw_value))
                            entry.saved = True
                            entry.pk = NumericSerializer.python_value(raw_key)
                            break
                    else:
                        raise IndexError
                else:
                    raw_value = cursor.get(NumericSerializer.db_value(key))
                    if raw_value is None:
                        raise IndexError
                    else:
                        entry = self.connection.model(
                            **ObjectSerializer.python_value(raw_value))
                        entry.saved = True
                        entry.pk = key 
                return entry
