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
            with self.connection.data_env() as env:
                with env.begin(write=False) as txn:
                    try:
                        entries_db = self.connection.get_db(env,
                                                            'entries_db_name',
                                                            txn)
                    except lmdb.ReadonlyError as exc:
                        raise StopIteration from exc

                    with txn.cursor(entries_db) as cursor:
                        for raw_key, raw_value in cursor:
                            entry = self.connection.model(
                                **ObjectSerializer.python_value(raw_value))
                            entry.pk = NumericSerializer.python_value(raw_key)
                            entry.saved = True
                            yield entry

        return _iter()

    def __reversed__(self):
        def _riter():
            with self.connection.data_env() as env:
                with env.begin(write=False) as txn:
                    try:
                       entries_db = self.connection.get_db(env,
                                                           'entries_db_name',
                                                           txn)
                    except lmdb.ReadonlyError as exc:
                        raise StopIteration from exc

                    with txn.cursor(entries_db) as cursor:
                        for raw_key, raw_value in cursor.iterprev():
                            entry = self.connection.model(
                                **ObjectSerializer.python_value(raw_value))
                            entry.saved = True
                            entry.pk = NumericSerializer.python_value(raw_key)
                            yield entry

        return _riter()

    @mask_exception(lmdb.ReadonlyError, IndexError)
    def __getitem__(self, key):
        with self.connection.data_env() as env:
            with env.begin(write=False) as txn:
                entries_db = self.connection.get_db(env,
                                                    'entries_db_name',
                                                    txn)
                with txn.cursor(entries_db) as cursor:
                    raw_value = cursor.get(NumericSerializer.db_value(key))
                    if raw_value is None:
                        raise IndexError
                    else:
                        entry = self.connection.model(
                            **ObjectSerializer.python_value(raw_value))
                        entry.saved = True
                        entry.pk = key 
                        return entry
