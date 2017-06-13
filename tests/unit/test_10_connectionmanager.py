import pytest


def test_connectionmanager_exists():
    try:
        from binlog.connectionmanager import ConnectionManager
    except ImportError as exc:
        assert False, exc
