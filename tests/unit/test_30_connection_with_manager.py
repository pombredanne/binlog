import asyncio

from binlog.exceptions import BadUsageError
from binlog.model import Model

import pytest


def test_nested_connections(tmpdir):
    with Model.open(tmpdir) as conn1:
        with Model.open(tmpdir) as conn2:
            assert conn1 is conn2


def test_not_nested_connections(tmpdir):
    with Model.open(tmpdir) as conn1:
        pass

    with Model.open(tmpdir) as conn2:
        assert conn1 is not conn2


def test_not_nested_inside_nested_connection(tmpdir):

    with Model.open(tmpdir) as conn1:

        with Model.open(tmpdir) as conn2_a:
            assert conn1 is conn2_a

        with Model.open(tmpdir) as conn2_b:
            assert conn1 is conn2_b


def test_nested_connections_different_parameters(tmpdir):
    with Model.open(tmpdir, map_async=True) as conn1:
        with pytest.raises(ValueError):
            with Model.open(tmpdir, map_async=False) as conn2:
                pass


def test_concurrent_access_is_same_connection(tmpdir):

    class Barrier:
        def __init__(self, n):
            self.n = n
            self.count = 0
            self.mutex = asyncio.Semaphore(1)
            self.barrier = asyncio.Semaphore(0)

        @asyncio.coroutine
        def wait(self):
            yield from self.mutex.acquire()
            self.count = self.count + 1
            self.mutex.release()
            if self.count == self.n:
                self.barrier.release()
            yield from self.barrier.acquire()
            self.barrier.release()

    connections = list()
    barrier = Barrier(2)

    @asyncio.coroutine
    def first_connection():
        with Model.open(tmpdir) as myconn:
            connections.append(myconn)
            yield from barrier.wait()

    @asyncio.coroutine
    def second_connection():
        with Model.open(tmpdir) as myconn:
            connections.append(myconn)
            yield from barrier.wait()

    loop = asyncio.get_event_loop()
    asyncio.async(first_connection())
    loop.run_until_complete(second_connection())

    assert len(connections) == 2
    assert connections[0] is connections[1]
