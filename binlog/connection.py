from collections import namedtuple
from contextlib import contextmanager

import lmdb

from .exceptions import IntegrityError, ReaderDoesNotExist
from .reader import Reader
from .serializer import NumericSerializer, ObjectSerializer


class Connection(namedtuple('_Connection', ('model', 'path', 'kwargs'))):
    def __init__(self, *_, **__):
        self.closed = False

    def close(self):
        self.closed = True

    def __enter__(self):
        return self

    def __exit__(self, *_, **__):
        self.close()

    @contextmanager
    def data_env(self):
        env = lmdb.open(str(self.path),
                        max_dbs=2 + len(self.model._indexes),
                        **self.kwargs)
        try:
            yield env
        finally:
            env.close()

    @contextmanager
    def checkpoint_env(self):
        env = lmdb.open(
            str(self.path + self.model._meta['checkpoint_env_suffix']),
            **self.kwargs)
        try:
            yield env
        finally:
            env.close()

    def create(self, **kwargs):
        with self.data_env() as env:
            with env.begin(write=True) as txn:
                config_db = env.open_db(
                    key=self.model._meta['config_db_name'].encode('utf-8'),
                    txn=txn)
                entries_db = env.open_db(
                    key=self.model._meta['entries_db_name'].encode('utf-8'),
                    txn=txn)

                # Get the next index
                with txn.cursor(config_db) as cursor:
                    next_idx_raw = cursor.get(b'next_event_idx', None)
                if next_idx_raw is None:
                    next_idx = 0
                else:
                    next_idx = NumericSerializer.python_value(next_idx_raw)
                
                entry = self.model(**kwargs)
                success = entry.save(next_idx, entries_db, txn)

                # Update next_idx
                with txn.cursor(config_db) as cursor:
                    cursor.put(b'next_event_idx',
                               NumericSerializer.db_value(next_idx + 1))

                if success:
                    return entry
                else:
                    raise IntegrityError("Key already exists")


    def get_db(self, env, meta_key, txn):
        return env.open_db(
            key=self.model._meta[meta_key].encode('utf-8'),
            txn=txn)

    def bulk_create(self, entries):
        with self.data_env() as env:
            with env.begin(write=True) as txn:
                config_db = self.get_db(env, 'config_db_name', txn)
                entries_db = self.get_db(env, 'entries_db_name', txn)

                # Get the next index
                with txn.cursor(config_db) as cursor:
                    next_idx_raw = cursor.get(b'next_event_idx', None)
                if next_idx_raw is None:
                    next_idx = 0
                else:
                    next_idx = NumericSerializer.python_value(next_idx_raw)
        
                def get_raw():
                    for pk, entry in enumerate(entries, next_idx):
                        entry.pk = pk
                        entry.saved = True
                        yield (NumericSerializer.db_value(pk),
                               ObjectSerializer.db_value(entry.copy()))

                with txn.cursor(entries_db) as cursor:
                    consumed, added = cursor.putmulti(get_raw(),
                                                      dupdata=False,
                                                      overwrite=False,
                                                      append=True)


                # Update next_idx
                with txn.cursor(config_db) as cursor:
                    cursor.put(b'next_event_idx',
                               NumericSerializer.db_value(next_idx + consumed))

                if consumed != added:
                    raise IntegrityError("Some key already exists")
                else:
                    return added

    def reader(self, name=None):
        if name is not None:
            with self.checkpoint_env() as env:
                with env.begin(write=False) as txn:
                    with txn.cursor() as cursor:
                        raw = cursor.get(name.encode('utf-8'))
                        if raw is None:
                            raise ReaderDoesNotExist
                        else:
                            config = ObjectSerializer.python_value(raw)
        else:
            config = None

        return Reader(self, name, config)

    def register_reader(self, name):
        with self.checkpoint_env() as env:
            with env.begin(write=True) as txn:
                with txn.cursor() as cursor:
                    success = cursor.put(name.encode('utf-8'),
                                         ObjectSerializer.db_value({}),
                                         overwrite=False)
                    return success
