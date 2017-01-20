import pytest

from binlog.database import Database
from binlog.serializer import NumericSerializer


def test_index_can_be_imported():
    try:
        from binlog.index import Index
    except ImportError as exc:
        assert False, exc


def test_index_is_database_subclass():
    from binlog.index import Index

    assert issubclass(Index, Database)


def test_index_key_is_numeric():
    from binlog.index import Index

    assert Index.V is NumericSerializer
