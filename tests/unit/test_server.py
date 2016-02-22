import pytest


@pytest.mark.wip
def test_Server():
    try:
        from binlog.server import Server
    except ImportError:
        assert False, "Cannot import Server."


@pytest.mark.wip
def test_Server_have_inner_binlog():
    from binlog.server import Server
    import tempfile

    with tempfile.TemporaryDirectory() as base:
        s = Server(base, "/tmp/server.sock")
        assert s.binlog.path == base


@pytest.mark.wip
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
def test_Server_run_is_coroutine():
    from binlog.server import Server
    import asyncio
    import tempfile

    with tempfile.TemporaryDirectory() as base:
        s = Server(base, "/tmp/server.sock")

        assert asyncio.iscoroutinefunction(s.run)
