import pytest


def test_database_exists():
    try:
        from binlog.database import Database
    except ImportError as exc:
        assert False, exc


def test_database_is_abstract():
    from binlog.database import Database

    with pytest.raises(TypeError):
        Database()

    class MyDatabase(Database):
        K = None
        V = None

    # SHOULD NOT RAISE
    d = MyDatabase()
