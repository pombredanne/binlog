from itertools import takewhile, islice

import lmdb

from .databases import Entries
from .serializer import NumericSerializer, ObjectSerializer
from .util import MaskException, cmp


class Reader:
    def __init__(self, connection, name, registry):
        self.connection = connection
        self.name = name
        self.registry = registry
        self.closed = False

    def is_acked(self, entry):
        if entry.saved and self.registry is not None and \
                entry.pk in self.registry:
            return True
        return False

    def close(self):
        self.commit()
        self.closed = True

    def commit(self):
        if self.registry:
            self.connection.save_registry(self.name, self.registry)

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

    def _iter(self, cursor_attr, *args, start=None, **kwargs):
        with MaskException(lmdb.ReadonlyError, StopIteration):
            with self.connection.data(write=False) as res:
                with Entries.cursor(res) as cursor:
                    if start is not None:
                        cursor.set_range(start)
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
        if isinstance(key, int):
            with self.connection.data(write=False) as res:
                with res.txn.cursor(res.db['entries']) as cursor:
                    if key < 0:
                        for idx, raw_item in enumerate(cursor.iterprev(), 1):
                            if key + idx == 0:
                                raw_key, raw_value = raw_item
                                entry = self.connection.model(
                                    **ObjectSerializer.python_value(raw_value))
                                entry.saved = True
                                entry.pk = NumericSerializer.python_value(
                                    raw_key)
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
        elif isinstance(key, slice):
            def to_num(v):
                return 0 if v is None else v

            def to_idx(v):
                try:
                    return self[v].pk if v is not None and v < 0 else v
                except IndexError:
                    return IndexError

            def are_numbers(*items):
                return all(isinstance(i, int) for i in items)

            if key.step == 0:
                raise ValueError("slice step cannot be zero")
            else:
                direction = 'iternext' if to_num(key.step) >= 0 else 'iterprev'

            start = to_idx(key.start)
            stop = to_idx(key.stop)
            step = abs(key.step) if key.step is not None else 1
            step_sign = (cmp(to_num(key.step), 0)
                         if key.step is not None
                         else 1)

            if stop is IndexError and step_sign == 1:
                it = []
            elif start is IndexError and step_sign == -1:
                it = []
            elif isinstance(start, int) and isinstance(stop, int) and cmp(start, stop) == step_sign:
                it = []
            else:
                it = self._iter(direction,
                                start=None if start is IndexError else start)


            if isinstance(stop, int) and step_sign is not None:
                if step_sign > 0:
                    it = takewhile(lambda i: i[0] < stop, it)
                else:
                    it = takewhile(lambda i: i[0] > stop, it)

            it = islice(it, None, None, step)

            return (self._to_model(*i) for i in it)
