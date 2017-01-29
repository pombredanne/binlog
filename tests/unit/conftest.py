import tempfile
from contextlib import contextmanager

import lmdb
import pytest



@pytest.fixture
def testenv():
    from binlog.abstract import Direction
    from binlog.connection import Resources
    from binlog.index import Index
    from binlog.serializer import NumericSerializer

    class TestIndex(Index):
        K = NumericSerializer
        V = NumericSerializer

    @contextmanager
    def _testenv(dupsort=False):
        with tempfile.TemporaryDirectory() as tmpdir:
            with lmdb.open(tmpdir, max_dbs=1, map_size=2**32) as env:
                @contextmanager
                def _transaction(direction=Direction.F):
                    with env.begin(write=True) as txn:
                        res = Resources(env,
                                        txn,
                                        db={'test': env.open_db(
                                                key=b'test',
                                                txn=txn,
                                                dupsort=dupsort)})

                        with TestIndex.cursor(res,
                                              db_name='test',
                                              direction=direction) as cursor:
                            yield cursor
                yield _transaction
    return _testenv
