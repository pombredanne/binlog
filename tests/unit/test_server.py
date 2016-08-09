from concurrent.futures import ThreadPoolExecutor
import collections
import os
import pytest
import socket
import tempfile

from hypothesis import strategies as st
from hypothesis import given

from binlog.reader import TDSReader


def test_Server():
    try:
        from binlog.server import Server
    except ImportError:
        assert False, "Cannot import Server."


def test_Server_have_inner_binlog():
    from binlog.server import Server
    import tempfile

    with tempfile.TemporaryDirectory() as base:
        s = Server(base, "/tmp/server.sock")
        assert s.binlog.path == base


def test_Server_get_protocol():
    from binlog.server import Server
    from asyncio import Protocol
    import tempfile

    with tempfile.TemporaryDirectory() as base:
        s = Server(base, "/tmp/server.sock")
        p1 = s.get_protocol()
        assert issubclass(p1, Protocol)

        p2 = s.get_protocol()
        assert p1 is p2


@pytest.mark.wip
def test_Server_protocol_store_in_binlog():
    from binlog.server import Server
    from binlog.reader import TDSReader
    import tempfile

    with tempfile.TemporaryDirectory() as base:
        s = Server(base, "/tmp/server.sock")

        p = s.get_protocol()()
        p.connection_made(None)
        p.data_received(b"TEST")
        p.connection_lost(None)

        r = TDSReader(base)
        assert r.next_record().value == b"TEST"


@pytest.mark.wip
def test_Server_protocol_not_store_in_binlog_when_no_data():
    from binlog.server import Server
    from binlog.reader import TDSReader
    import tempfile

    with tempfile.TemporaryDirectory() as base:
        s = Server(base, "/tmp/server.sock")

        p = s.get_protocol()()
        p.connection_made(None)
        p.connection_lost(None)

        r = TDSReader(base)
        assert r.next_record() is None


@pytest.mark.wip
def test_Server_protocol_not_store_in_binlog_when_data_is_empty():
    from binlog.server import Server
    from binlog.reader import TDSReader
    import tempfile

    with tempfile.TemporaryDirectory() as base:
        s = Server(base, "/tmp/server.sock")

        p = s.get_protocol()()
        p.connection_made(None)
        p.data_received(b"")
        p.connection_lost(None)

        r = TDSReader(base)
        assert r.next_record() is None


@given(data=st.lists(st.binary(min_size=1), min_size=1))
def test_Server_concurrent_writes(server_factory, data):

    with server_factory() as server:
        socket_path, env_path = server
        def storedatum(d):
            s = socket.socket(socket.AF_UNIX)
            s.connect(socket_path)
            s.sendall(d)
            s.close()

        with ThreadPoolExecutor(max_workers=64) as pool:
            for res in pool.map(storedatum, data):
                assert res is None

    expected_set = collections.Counter(data)

    reader = TDSReader(env_path)
    current_set = collections.Counter(
        d.value for d in iter(reader.next_record, None))

    assert current_set == expected_set
