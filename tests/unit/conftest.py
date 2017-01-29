from bisect import bisect_left, bisect_right
from collections import deque
from contextlib import contextmanager
import tempfile

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


@pytest.fixture
def dummyiterseek():
    from binlog.abstract import IterSeek, Direction

    class DummyIterSeek(IterSeek):
        def __init__(self, items, direction=Direction.F):
            self.items = list(sorted(items))
            self.direction = direction

            if direction is Direction.F:
                self.left = deque()
                self.right = deque(self.items)
                self.bisect = bisect_left
            else:
                self.left = deque(self.items)
                self.right = deque()
                self.bisect = bisect_right

        def __next__(self):
            if self.direction is Direction.F:
                try:
                    item = self.right.popleft()
                except IndexError:
                    raise StopIteration
                self.left.append(item)
                return item
            else:
                try:
                    item = self.left.pop()
                except IndexError:
                    raise StopIteration
                self.right.appendleft(item)
                return item

        def seek(self, value):
            idx = self.bisect(self.items, value)
            self.left, self.right = (deque(self.items[:idx]),
                                     deque(self.items[idx:]))

    return DummyIterSeek
