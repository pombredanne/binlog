from collections import namedtuple
from contextlib import contextmanager
from functools import reduce, wraps
from itertools import islice
from pathlib import Path
import operator as op
import os
import threading

import lmdb

from .databases import Config, Checkpoints, Entries
from .databases import Registry as RegistryDB
from .exceptions import IntegrityError, ReaderDoesNotExist, BadUsageError
from .reader import Reader
from .registry import Registry
from .util import MaskException


Resources = namedtuple('Resources', ['env', 'txn', 'db'])


class DBOpener:
    def __init__(self, env, txn, **kwargs):
        self.env = env
        self.txn = txn
        self.kwargs = kwargs
        self._dbs = {}

    def __getitem__(self, name):
        if name not in self._dbs:
            self._dbs[name] = self.env.open_db(
                key=name.encode('utf-8'),
                txn=self.txn,
                **self.kwargs)

        return self._dbs[name]


def same_thread(f):
    @wraps(f)
    def wrapper(self, *args, **kwargs):
        if self.tid != threading.current_thread():
            raise BadUsageError(
                ("Sharing connections among threads is not supported."))
        elif self.pid != os.getpid():
            raise BadUsageError(
                ("Sharing connections among processes is not supported."))
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


class Connection:
    def __init__(self, model, path, kwargs):
        self.model = model
        self.path = path
        self.kwargs = kwargs

        self.closed = None
        self._data_env = None
        self._readers_env = None
        self.refcount = 0

        self.pid = os.getpid()
        self.tid = threading.current_thread()
        if self.tid != threading.main_thread():
            raise BadUsageError(
                ("This version doesn't support using connections "
                 "outside the main thread."))

    def open(self):
        if self.closed:
            raise BadUsageError("Cannot reuse a closed connection.")
        elif self.refcount == 0:
            self._open_environments()
            self.refcount = 1
            self.closed = False
            return self
        else:
            self.refcount += 1
            return self

    def _open_environments(self):
        """
        Access to data_env and readers_env properties to open the lmdb
        environments.

        """
        # Open DATA ENV
        self.data_env = lmdb.open(
            self._gen_path('data_env_directory'),
            max_dbs=2 + len(self.model._indexes),
            **self.kwargs)

        # Open READERS ENV
        self.readers_env = lmdb.open(
            self._gen_path('readers_env_directory'),
            max_dbs=200,
            **self.kwargs)

    def close(self):
        if self.refcount == 1:
            self.closed = True

            # DATA ENV
            self.data_env.close()
            self.data_env = None

            # READERS ENV
            self.readers_env.close()
            self.readers_env = None

            self.refcount -= 1

            # This MUST be imported every time because can be invalidated by
            # `reset_connections`.
            from .connectionmanager import PROCESS_CONNECTIONS
            PROCESS_CONNECTIONS.close(self.path)

        elif self.refcount < 1:
            raise BadUsageError("Cannot close already closed connection")
        else:  # self.refcount > 1
            self.refcount -= 1

    def __enter__(self):
        if self.tid != threading.current_thread():
            raise RuntimeError("Connection made from a different thread!")
        elif self.pid != os.getpid():
            raise BadUsageError(
                ("Sharing connections among processes is not supported."))
        else:
            return self

    def __exit__(self, *_, **__):
        self.close()

    def __getstate__(self):
        raise BadUsageError("Connection cannot be picklelized.")

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
#            checkpoints_db = self._get_db(env, txn, 'checkpoints_db_name')
            yield Resources(env=env,
                            txn=txn,
                            db=DBOpener(env, txn))

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
    def _drop_indexes(self):
        with self.data(write=True) as res:
            for index_name, index in self.model._indexes.items():
                db_name = self._get_index_name(index_name)
                res.txn.drop(res.db[db_name], delete=True)

    @open_db
    @same_thread
    def _reindex(self):
        with self.data(write=True) as res:
            with Entries.cursor(res) as cursor:
                found = cursor.first()
                if found:
                    for key, value in cursor.iternext():
                        entry = self.model(**value)
                        entry.pk = key
                        entry.saved = True
                        self._index(res, entry)
                        print(".", end="", flush=True)

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
        if name is not None and name not in self.list_readers():
            raise ReaderDoesNotExist("%s reader does not exists" % name)

        from binlog.registry import MemoryCachedDBRegistry
        from binlog.abstract import Direction

        registry = MemoryCachedDBRegistry(
            name=name,
            connection=self,
            direction=Direction.F) if name in self.list_readers() else None

        return Reader(self, name, registry)

    @open_db
    @same_thread
    def register_reader(self, name, content=None):
        if name in self.list_readers():
            return False
        else:
            path = name.split('.')

            with self.readers(write=True) as res:
                with RegistryDB.named(name).cursor(res) as cursor:
                    if content is not None:
                        raise NotImplementedError("XXX")
                    result = True

            parents = path[:-1]
            if not parents:
                return result
            else:
                parents_result = self.register_reader('.'.join(parents))
                return result or parents_result

    @open_db
    @same_thread
    def clone_reader(self, src, dst):
        readers = self.list_readers()
        if src not in readers:
            raise ReaderDoesNotExist("%s reader does not exists." % src)
        elif dst in readers:
            raise RuntimeError("%s reader already exists." % dst)
        else:
            with self.readers(write=True) as res:
                with RegistryDB.named(src).cursor(res) as scursor:
                    with RegistryDB.named(dst).cursor(res) as dcursor:
                        dcursor.putmulti(scursor.iternext())

    @open_db
    @same_thread
    @MaskException(lmdb.ReadonlyError, ReaderDoesNotExist)
    def unregister_reader(self, name):
        if name not in self.list_readers():
            raise ReaderDoesNotExist("%s reader does not exists" % name)
        else:
            with self.readers(write=True) as res:
                res.txn.drop(res.db[name])
                return True

    @open_db
    @same_thread
    def save_registry(self, name, added):
        with self.readers(write=True) as res:
            with RegistryDB.named(name).cursor(res) as cursor:
                for s in added.acked:
                    s_L = max([s.MIN, s.L - 1])
                    s_R = min([s.MAX, s.R + 1])

                    if cursor.set_range(s_L):
                        f_R, f_L = cursor.item()
                        if f_L <= s_L:
                            first = (f_L, f_R)
                        else:
                            first = None
                    else:
                        first = None

                    if cursor.set_range(s_R):
                        l_R, l_L = cursor.item()
                        if l_L <= s_R:
                            last = (l_L, l_R)
                        else:
                            last = None
                    else:
                        last = None

                    if first is None and last is None:
                        cursor.put(s.R, s.L)
                    else:
                        if last is None:
                            cursor.get(f_R)
                            while cursor.delete2():
                                pass
                            cursor.put(s.R, f_L)
                        elif first is None:
                            while cursor.first():
                                c_R, c_L = cursor.item()
                                cursor.delete2()
                                if (c_L, c_R) == (l_L, l_R):
                                    break
                            cursor.put(l_R, s.L)
                        else:
                            # Both present
                            if first == last:
                                # This means is already in the segment, so
                                # already acked. Nothing to do here.
                                pass
                            else:
                                cursor.get(f_R)
                                while cursor.delete2():
                                    c_R, c_L = cursor.item()
                                    if (c_L, c_R) == (l_L, l_R):
                                        cursor.delete2()
                                        break
                                cursor.put(l_R, f_L)
                return True

    @open_db
    @same_thread
    def list_readers(self):
        with self.readers(write=True) as res:
            with res.txn.cursor() as cursor:
                return [bytes(x).decode("utf-8") for x in cursor.iternext(values=False)]

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
