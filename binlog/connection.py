from collections import namedtuple
from contextlib import contextmanager

import lmdb

from .serializer import NumericSerializer


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
    def _lmdb_env(self):
        env = lmdb.open(str(self.path),
                        max_dbs=2 + len(self.model._indexes),
                        **self.kwargs)
        try:
            yield env
        finally:
            env.close()


    def create(self, **kwargs):
        with self._lmdb_env() as env:
            with env.begin(write=True) as txn:
                metadb = env.open_db(
                    key=self.model._meta['metadb_name'].encode('utf-8'),
                    txn=txn)
                entriesdb = env.open_db(
                    key=self.model._meta['entriesdb_name'].encode('utf-8'),
                    txn=txn)

                # Get the next index
                with txn.cursor(metadb) as cursor:
                    next_idx_raw = cursor.get(b'next_event_idx', None)
                if next_idx_raw is None:
                    next_idx = 0
                else:
                    next_idx = NumericSerializer.python_value(next_idx_raw)
                
                entry = self.model(**kwargs)
                success = entry.save(next_idx, entriesdb, txn)

                # Update next_idx
                with txn.cursor(metadb) as cursor:
                    cursor.put(b'next_event_idx',
                               NumericSerializer.db_value(next_idx + 1))

                return success, entry
