import pytest


def test_databases_exists():
    try:
        from binlog.databases import Database
    except ImportError as exc:
        assert False, exc


def test_databases_is_abstract():
    from binlog.databases import Database

    with pytest.raises(TypeError):
        Database()

    class MyDatabase(Database):
        K = None
        V = None

    # SHOULD NOT RAISE
    d = MyDatabase()
