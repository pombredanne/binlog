from contextlib import ExitStack
from itertools import takewhile, islice
import json

import lmdb

from .abstract import Direction
from .databases import Entries, Hints
from .serializer import NumericSerializer, ObjectSerializer
from .util import MaskException, cmp
from .registry import RegistryIterSeek, Registry


class Reader:
    def __init__(self, connection, name, registry):
        self.connection = connection
        self.name = name
        self.registry = registry
        self._parent = None

        self.closed = False

    @property
    def parent(self):
        if self.name is None:
            return None

        if self._parent is None:
            parent_name = '.'.join(self.name.split('.')[:-1])
            if parent_name:
                self._parent = self.connection.reader(parent_name)
            else:
                self._parent = self

        if self._parent is self:
            return None
        else:
            return self._parent

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

        if self.parent is not None:
            self.parent.commit()

    def __enter__(self):
        return self

    def __exit__(self, *_, **__):
        self.close()

    def ack(self, entry):
        # FIXME: import on top, fix recursive import
        from .model import Model

        if self.registry is None:
            raise RuntimeError("Cannot ACK events on anonymous reader.")

        if isinstance(entry, int):
            return self.registry.add(entry)
        elif not isinstance(entry, Model):
            raise TypeError("ACK accepts either pk or model instance")
        elif not entry.saved:
            raise ValueError("Entry must be saved first")
        else:
            return self.registry.add(entry.pk)

    def recursive_ack(self, entry):
        if self.parent is None:
            return self.ack(entry)
        else:
            return any((self.parent.recursive_ack(entry),
                        self.ack(entry)))

    def ack_from_filter(self, recursive=False, limit=None, **filters):
        with MaskException(lmdb.Error, RuntimeError):
            with MaskException(lmdb.ReadonlyError, RuntimeError):
                with self.connection.data(write=False) as res:
                    it = self.__iterseek__(direction=Direction.F)
                    with ExitStack() as index_filter:
                        for key, value in filters.items():
                            index = self.connection.model._indexes.get(key)
                            if index is None:
                                raise ValueError(
                                    ("Cannot ack from filter with a"
                                     " non-indexed field."))
                            else:
                                db_name = self.connection._get_index_name(key)
                                if isinstance(value, list):
                                    it_or = None
                                    for v in value:
                                        index = self.connection.model._indexes.get(key)
                                        index_cursor = index_filter.enter_context(
                                            index.cursor(res, db_name=db_name))
                                        try:
                                            index_cursor.dupkey = v
                                        except ValueError:
                                            continue
                                        else:
                                            if it_or is None:
                                                it_or = index_cursor
                                            else:
                                                it_or |= index_cursor
                                    if it_or is not None:
                                        it &= it_or
                                else:
                                    index_cursor = index_filter.enter_context(
                                        index.cursor(res, db_name=db_name))
                                    try:
                                        index_cursor.dupkey = value
                                    except ValueError:
                                        return
                                    else:
                                        it &= index_cursor

                        hint = self._load_hint(**filters)
                        if hint is not None:
                            it.seek(hint)

                        pk = None
                        for n, pk in enumerate(it):

                            if limit is not None and n >= limit:
                                break

                            if recursive:
                                self.recursive_ack(pk)
                            else:
                                self.ack(pk)

                        if pk is None:
                            try:
                                self._save_hint(self[-1].pk, **filters)
                            except IndexError:
                                pass
                        else:
                            self._save_hint(pk, **filters)

    def _hint_key(self, attrs):
        return ":".join([self.name, json.dumps(attrs, sort_keys=True)])

    def _load_hint(self, **filters):
        """Load an initial position for a given search."""
        try:
            with self.connection.readers(write=False) as res:
                with Hints.cursor(res) as cursor:
                    key = self._hint_key(filters)
                    hint = cursor.get(key, default=None)
        except lmdb.ReadonlyError:
            return None

    def _save_hint(self, hint, **filters):
        """Load an initial position for a given search."""
        try:
            with self.connection.readers(write=True) as res:
                with Hints.cursor(res) as cursor:
                    key = self._hint_key(filters)
                    return cursor.put(key, hint)
        except lmdb.ReadonlyError:
            return None

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

    def __iterseek__(self, direction):
        from .registry import DBRegistry, MemoryCachedDBRegistry
        if self.name is None:
            return RegistryIterSeek(~Registry(), direction=direction)
        else:
            return MemoryCachedDBRegistry(
                connection=self.connection,
                name=self.name,
                direction=direction,
                inverted=True,
                registry=self.registry.memory.registry)
        # if self.registry is None:
        # else:
        #     return RegistryIterSeek(~self.registry, direction=direction)

    def __iter__(self):
        with MaskException(lmdb.ReadonlyError, StopIteration):
            with self.connection.data(write=False) as res:
                with Entries.cursor(res) as cursor:
                    it = cursor & self.__iterseek__(direction=Direction.F)
                    for pk in it:
                        try:
                            yield self[pk]
                        except IndexError:
                            pass

    def __reversed__(self):
        with MaskException(lmdb.ReadonlyError, StopIteration):
            with self.connection.data(write=False) as res:
                with Entries.cursor(res, direction=Direction.B) as cursor:
                    it = cursor & self.__iterseek__(direction=Direction.B)
                    for pk in it:
                        try:
                            yield self[pk]
                        except IndexError:
                            pass

    def filter(self, **filters):
        with MaskException(lmdb.Error, StopIteration):
            with MaskException(lmdb.ReadonlyError, StopIteration):
                with self.connection.data(write=False) as res:
                    with Entries.cursor(res) as cursor:
                        it = cursor & self.__iterseek__(direction=Direction.F)
                        non_index_filter = {}
                        with ExitStack() as index_filter:
                            for key, value in filters.items():
                                index = self.connection.model._indexes.get(key)
                                if index is None:
                                    non_index_filter[key] = value
                                else:
                                    db_name = self.connection._get_index_name(key)
                                    index_cursor = index_filter.enter_context(
                                        index.cursor(res, db_name=db_name))
                                    try:
                                        index_cursor.dupkey = value
                                    except ValueError:
                                        return
                                    else:
                                        it &= index_cursor

                            for pk in it:
                                try:
                                    entry = self[pk]
                                except IndexError:
                                    pass
                                else:
                                    for key, value in non_index_filter.items():
                                        if entry.get(key) != value:
                                            break
                                    else:
                                        yield entry

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
            elif are_numbers(start, stop) and cmp(start, stop) == step_sign:
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
        else:
            raise TypeError("Item must be int or slice.")
