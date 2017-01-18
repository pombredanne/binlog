import pytest


def test_index_can_be_imported():
    try:
        from binlog.index import Index
    except ImportError as exc:
        assert False, exc
