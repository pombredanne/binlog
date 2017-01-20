from collections import namedtuple
from contextlib import contextmanager
from pathlib import Path

import lmdb

from .database import Config, Checkpoints, Entries
from .exceptions import IntegrityError, ReaderDoesNotExist
from .reader import Reader
from .registry import Registry
from .util import MaskException


Resources = namedtuple('Resources', ['env', 'txn', 'db'])


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

    def _get_db(self, env, txn, meta_key, **kwargs):
        return env.open_db(key=self.model._meta[meta_key].encode('utf-8'),
                           txn=txn, **kwargs)

    def _gen_path(self, metaname):
        basename = Path(str(self.path))
        dirname = Path(self.model._meta['data_env_directory'])
        return str(basename / dirname)

    @contextmanager
    def data(self, write=True):
        path = self._gen_path('data_env_directory')
        max_dbs = 2 + len(self.model._indexes)

        with self._open_env(path, max_dbs=max_dbs, **self.kwargs) as env:
            with env.begin(write=write, buffers=True) as txn:
                config_db = self._get_db(env, txn, 'config_db_name')
                entries_db = self._get_db(env, txn, 'entries_db_name')
                yield Resources(env=env, txn=txn, db={'config': config_db,
                                                      'entries': entries_db})

    @contextmanager
    def readers(self, write=True):
        path = self._gen_path('readers_env_directory')
        max_dbs = 1

        with self._open_env(path, max_dbs=max_dbs, **self.kwargs) as env:
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
                return entry
            else:
                raise IntegrityError("Key already exists")

    def bulk_create(self, entries):
        with self.data(write=True) as res:
            next_idx = self._get_next_event_idx(res)

            def get_raw():
                for pk, entry in enumerate(entries, next_idx):
                    entry.mark_as_saved(pk)
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

    def register_reader(self, name):
        with self.readers(write=True) as res:
            with Checkpoints.cursor(res) as cursor:
                return cursor.put(name, Registry(), overwrite=False)

    def save_registry(self, name, registry):
        with self.readers(write=True) as res:
            with Checkpoints.cursor(res) as cursor:
                return cursor.put(name, registry, overwrite=True)
