from collections import namedtuple
from contextlib import contextmanager

import lmdb

from .exceptions import IntegrityError, ReaderDoesNotExist
from .reader import Reader
from .serializer import Checkpoint, NextEventID


ConnectionResources = namedtuple('ConnectionResources',
                                 ['env', 'txn', 'db'])


class Connection(namedtuple('_Connection', ('model', 'path', 'kwargs'))):
    def __init__(self, *_, **__):
        self.closed = False

    def close(self):
        self.closed = True

    def __enter__(self):
        return self

    def __exit__(self, *_, **__):
        self.close()

    def _open_env(self, path, **kwargs):
        return lmdb.open(str(path), **kwargs)

    def data_env(self):
        path = self.path
        max_dbs = 2 + len(self.model._indexes)

        return self._open_env(path, max_dbs=max_dbs, **self.kwargs)

    def checkpoint_env(self):
        path = self.path + self.model._meta['checkpoint_env_suffix']
        max_dbs = 1

        return self._open_env(path, max_dbs=max_dbs, **self.kwargs)

    def get_db(self, env, txn, meta_key):
        return env.open_db(key=self.model._meta[meta_key].encode('utf-8'),
                           txn=txn)

    @contextmanager
    def _data(self, write=True):
        with self.data_env() as env:
            with env.begin(write=write) as txn:
                yield ConnectionResources(
                    env=env,
                    txn=txn,
                    db={'config': self.get_db(env, txn, 'config_db_name'),
                        'entries': self.get_db(env, txn, 'entries_db_name')})

    @contextmanager
    def _checkpoints(self, write=True):
        with self.checkpoint_env() as env:
            with env.begin(write=write) as txn:
                yield ConnectionResources(env=env, txn=txn, db={})

    def _get_next_event_idx(self, res):
        # Get the next index
        with res.txn.cursor(res.db['config']) as cursor:
            next_idx_raw = cursor.get(NextEventID.K, None)
        if next_idx_raw is None:
            return 0
        else:
            return NextEventID.V.python_value(next_idx_raw)

    def _update_next_event_idx(self, res, value):
        # Update next_idx
        with res.txn.cursor(res.db['config']) as cursor:
            cursor.put(NextEventID.K, NextEventID.V.db_value(value))

    def create(self, **kwargs):
        with self._data(write=True) as res:
            next_idx = self._get_next_event_idx(res)
            
            entry = self.model(**kwargs)
            success = entry.save(next_idx, res.db['entries'], res.txn)

            self._update_next_event_idx(res, next_idx + 1)

            if success:
                return entry
            else:
                raise IntegrityError("Key already exists")

    def bulk_create(self, entries):
        with self._data(write=True) as res:
            next_idx = self._get_next_event_idx(res)

            def get_raw():
                for pk, entry in enumerate(entries, next_idx):
                    entry.mark_as_saved(pk)

                    yield (self.model.K.db_value(pk),
                           self.model.V.db_value(entry.copy()))

            with res.txn.cursor(res.db['entries']) as cursor:
                consumed, added = cursor.putmulti(get_raw(),
                                                  dupdata=False,
                                                  overwrite=False,
                                                  append=True)

            self._update_next_event_idx(res, next_idx + consumed)

            if consumed != added:
                raise IntegrityError("Some key already exists")
            else:
                return added

    def reader(self, name=None):
        if name is not None:
            with self._checkpoints(write=False) as res:
                with res.txn.cursor() as cursor:
                    raw = cursor.get(Checkpoint.K.db_value(name))
                    if raw is None:
                        raise ReaderDoesNotExist
                    else:
                        config = Checkpoint.V.python_value(raw)
        else:
            config = None

        return Reader(self, name, config)

    def register_reader(self, name):
        with self._checkpoints(write=True) as res:
            with res.txn.cursor() as cursor:
                success = cursor.put(Checkpoint.K.db_value(name),
                                     Checkpoint.V.db_value({}),
                                     overwrite=False)
                return success
