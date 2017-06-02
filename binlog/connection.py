from collections import namedtuple
from contextlib import contextmanager
from functools import reduce, wraps
from itertools import islice
from pathlib import Path
import operator as op
import threading

import lmdb

from .databases import Config, Checkpoints, Entries
from .exceptions import IntegrityError, ReaderDoesNotExist, BadUsageError
from .reader import Reader
from .registry import Registry
from .util import MaskException


Resources = namedtuple('Resources', ['env', 'txn', 'db'])


def same_thread(f):
    @wraps(f)
    def wrapper(self, *args, **kwargs):
        if self.thread_id != threading.current_thread():
            raise BadUsageError(
                ("Sharing connections among threads is not supported."))
        else:
            return f(self, *args, **kwargs)

    return wrapper


def open_db(f):
    @wraps(f)
    def wrapper(self, *args, **kwargs):
        if self.closed:
            raise BadUsageError("Cannot use a closed database.")
        else:
            return f(self, *args, **kwargs)

    return wrapper


class Connection(namedtuple('_Connection', ('model', 'path', 'kwargs'))):
    def __init__(self, *_, **__):
        self.closed = False
        self._data_env = None
        self._readers_env = None

        self.thread_id = threading.current_thread()
        if self.thread_id != threading.main_thread():
            raise BadUsageError(
                ("This version doesn't support using connections "
                 "outside the main thread."))

    def _open_environments(self):
        """
        Access to data_env and readers_env properties to open the lmdb
        environments.

        """
        self.data_env
        self.readers_env
        self.closed = False

    def close(self):
        self.closed = True
        del self.data_env
        del self.readers_env

    def __enter__(self):
        if self.thread_id != threading.current_thread():
            raise RuntimeError("Connection made from a different thread!")
        else:
            self._open_environments()
            return self

    def __exit__(self, *_, **__):
        self.close()

    def __getstate__(self):
        raise BadUsageError("Connection cannot be picklelized.")

    def _open_env(self, path, **kwargs):
        return lmdb.open(str(path), **kwargs)

    @property
    def data_env(self):
        if self._data_env is None:
            path = self._gen_path('data_env_directory')
            max_dbs = 2 + len(self.model._indexes)
            self._data_env = self._open_env(path,
                                            max_dbs=max_dbs,
                                            **self.kwargs)
        return self._data_env

    @data_env.deleter
    def data_env(self):
        if self._data_env is not None:
            self._data_env.close()
            self._data_env = None

    @property
    def readers_env(self):
        if self._readers_env is None:
            path = self._gen_path('readers_env_directory')
            max_dbs = 1
            self._readers_env = self._open_env(path,
                                            max_dbs=max_dbs,
                                            **self.kwargs)
        return self._readers_env

    @readers_env.deleter
    def readers_env(self):
        if self._readers_env is not None:
            self._readers_env.close()
            self._readers_env = None

    def _get_db(self, env, txn, meta_key, **kwargs):
        return env.open_db(key=self.model._meta[meta_key].encode('utf-8'),
                           txn=txn, **kwargs)

    def _get_idx(self, env, txn, db_name, **kwargs):
        return env.open_db(
            key=db_name.encode('utf-8'),
            txn=txn,
            **kwargs)

    def _gen_path(self, metaname):
        basename = Path(str(self.path))
        dirname = Path(self.model._meta[metaname])
        return str(basename / dirname)

    def _get_index_name(self, name):
        template = self.model._meta['index_db_format']
        return template.format(model=self.model, index_name=name)

    @open_db
    @same_thread
    @contextmanager
    def data(self, write=True):
        env = self.data_env
        with env.begin(write=write, buffers=True) as txn:
            dbs = {}
            dbs['config'] = self._get_db(env, txn, 'config_db_name')
            dbs['entries'] = self._get_db(env, txn, 'entries_db_name')
            for index_name in self.model._indexes:
                index_db_name = self._get_index_name(index_name)
                dbs[index_db_name] = self._get_idx(env, txn, index_db_name,
                                                  dupsort=True)

            yield Resources(env=env, txn=txn, db=dbs)

    @open_db
    @same_thread
    @contextmanager
    def readers(self, write=True):
        env = self.readers_env
        with env.begin(write=write, buffers=True) as txn:
            checkpoints_db = self._get_db(env, txn, 'checkpoints_db_name')
            yield Resources(env=env,
                            txn=txn,
                            db={'checkpoints': checkpoints_db})

    def _get_next_event_idx(self, res):
        with Config.cursor(res) as cursor:
            return cursor.get('next_event_id', default=0)

    def _update_next_event_idx(self, res, value):
        with Config.cursor(res) as cursor:
            return cursor.put('next_event_id', value, overwrite=True)

    def _index(self, res, entry):
        for index_name, index in self.model._indexes.items():
            db_name = self._get_index_name(index_name)
            with index.cursor(res, db_name=db_name) as cursor:
                key = entry.get(index_name)
                if index.mandatory and key is None:
                    raise ValueError("value %s is mandatory" % index_name)
                elif key is not None:
                    value = entry.pk
                    if not cursor.put(key,
                                      value,
                                      overwrite=True,
                                      dupdata=True):
                        raise RuntimeError("Cannot index %s=%s" % (key, value))

    def _unindex(self, res, entry):
        for index_name, index in self.model._indexes.items():
            db_name = self._get_index_name(index_name)
            with index.cursor(res, db_name=db_name) as cursor:
                key = entry.get(index_name)
                if key is not None:  # pragma: no branch
                    value = entry.pk
                    cursor.delete(key, value)


    @open_db
    @same_thread
    def create(self, **kwargs):
        with self.data(write=True) as res:
            next_idx = self._get_next_event_idx(res)

            entry = self.model(**kwargs)
            with Entries.cursor(res) as cursor:
                success = cursor.put(next_idx,
                                     entry.copy(),
                                     overwrite=False,
                                     append=True)

            self._update_next_event_idx(res, next_idx + 1)

            if success:
                entry.pk = next_idx
                entry.saved = True
                self._index(res, entry)
                return entry
            else:
                raise IntegrityError("Key already exists")

    @open_db
    @same_thread
    def bulk_create(self, entries):
        with self.data(write=True) as res:
            next_idx = self._get_next_event_idx(res)

            def get_raw():
                for pk, entry in enumerate(entries, next_idx):
                    entry.mark_as_saved(pk)
                    self._index(res, entry)
                    yield (pk, entry.copy())

            with Entries.cursor(res) as cursor:
                consumed, added = cursor.putmulti(get_raw(),
                                                  dupdata=False,
                                                  overwrite=False,
                                                  append=True)

            self._update_next_event_idx(res, next_idx + consumed)

            if consumed != added:
                raise IntegrityError("Some key already exists")
            else:
                return added

    @open_db
    @same_thread
    @MaskException(lmdb.ReadonlyError, ReaderDoesNotExist)
    def reader(self, name=None):
        if name is not None:
            with self.readers(write=False) as res:
                with Checkpoints.cursor(res) as cursor:
                    registry = cursor.get(name, default=None)
                    if registry is None:
                        raise ReaderDoesNotExist
        else:
            registry = None

        return Reader(self, name, registry)

    @open_db
    @same_thread
    def register_reader(self, name):
        path = name.split('.')

        with self.readers(write=True) as res:
            with Checkpoints.cursor(res) as cursor:
                result = cursor.put(name, Registry(), overwrite=False)

        parents = path[:-1]
        if not parents:
            return result
        else:
            parents_result = self.register_reader('.'.join(parents))
            return result or parents_result

    @open_db
    @same_thread
    @MaskException(lmdb.ReadonlyError, ReaderDoesNotExist)
    def unregister_reader(self, name):
        with self.readers(write=True) as res:
            with Checkpoints.cursor(res) as cursor:
                if cursor.pop(name) is None:
                    raise ReaderDoesNotExist
                else:
                    return True

    @open_db
    @same_thread
    def save_registry(self, name, new_registry):
        with self.readers(write=True) as res:
            with Checkpoints.cursor(res) as cursor:
                stored_registry = cursor.get(name, default=Registry())
                return cursor.put(name,
                                  stored_registry | new_registry,
                                  overwrite=True)

    @open_db
    @same_thread
    def list_readers(self):
        with self.readers(write=True) as res:
            with Checkpoints.cursor(res) as cursor:
                return list(cursor.iternext(values=False))

    @open_db
    @same_thread
    def remove(self, entry):
        readers = self.list_readers()
        if not readers:
            raise ReaderDoesNotExist

        for name in readers:
            with self.reader(name) as reader:
                if not reader.is_acked(entry):
                    return False
        else:
            with self.data(write=True) as res:
                with Entries.cursor(res) as cursor:
                    success = cursor.pop(entry.pk) is not None
                    if success:
                        self._unindex(res, entry)
                    return success

    @open_db
    @same_thread
    def purge(self, chunk_size=1000):
        if chunk_size < 1:
            raise ValueError("chunk_size must be greater than 0")

        registries = [self.reader(name).registry
                      for name in self.list_readers()]
        removed = not_found = 0
        if registries:
            common_acked = iter(reduce(op.and_, registries))
            idx = chunk_size
            while idx == chunk_size:
                idx = 0
                it = islice(common_acked, 0, chunk_size)
                with self.data(write=True) as res:
                    with Entries.cursor(res) as cursor:
                        for idx, pk in enumerate(it, 1):
                            value = cursor.pop(pk)
                            if value is not None:
                                removed += 1
                                self._unindex(res, self.model(**value))
                            else:
                                not_found += 1
        return removed, not_found
