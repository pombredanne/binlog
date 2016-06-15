import pytest


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
