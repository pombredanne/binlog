from collections import namedtuple

import lmdb

from .database import Entries
from .serializer import NumericSerializer, ObjectSerializer
from .util import MaskException


class Reader:
    def __init__(self, connection, name, registry):
        self.connection = connection
        self.name = name
        self.registry = registry
        self.closed = False

    def close(self):
        if self.registry:
            self.connection.save_registry(self.name, self.registry)
        self.closed = True

    def __enter__(self):
        return self

    def __exit__(self, *_, **__):
        self.close()

    def ack(self, entry):
        if self.registry is None:
            raise RuntimeError("Cannot ACK events on anonymous reader.")
        elif not entry.saved:
            raise ValueError("Entry must be saved first")
        else:
            return self.registry.add(entry.pk)

    def _iter(self, cursor_attr, *args, **kwargs):
        with MaskException(lmdb.ReadonlyError, StopIteration):
            with self.connection.data(write=False) as res:
                with Entries.cursor(res) as cursor:
                    it = getattr(cursor, cursor_attr)(*args, **kwargs)
                    for key, value in it:
                        if self.registry is None or key not in self.registry:
                            yield (key, value)

    def _to_model(self, key, value):
        entry = self.connection.model(**value)
        entry.pk = key
        entry.saved = True
        return entry

    def __iter__(self):
        for key, value in self._iter('iternext'):
            yield self._to_model(key, value)

    def __reversed__(self):
        for key, value in self._iter('iterprev'):
            yield self._to_model(key, value)

    def filter(self, **filters):
        for key, value in self._iter('iternext'):
            for f_key, f_value in filters.items():
                if not f_key in value or value[f_key] != f_value:
                    break
            else:
                yield self._to_model(key, value)

    @MaskException(lmdb.ReadonlyError, IndexError)
    def __getitem__(self, key):
        with self.connection.data(write=False) as res:
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
