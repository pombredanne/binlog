from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from unittest.mock import patch
import pickle

import pytest

from binlog.model import Model
from binlog.exceptions import BadUsageError


def test_model_open_returns_connection(tmpdir):
    from binlog.connection import Connection

    conn = Model.open(tmpdir)

    assert isinstance(conn, Connection)


def test_connection_attributes(tmpdir):

    class CustomModel(Model):
        pass

    kwargs = {'somevar': 'somevalue'}
    with patch('lmdb.open') as lmdbopen:
        conn = CustomModel.open(tmpdir, **kwargs)

        assert conn.model is CustomModel
        assert conn.path == tmpdir
        assert conn.kwargs == kwargs
        assert not conn.closed

        assert not lmdbopen.called, "Should be called on __enter__"


def test_connection_close(tmpdir):
    conn = Model.open(tmpdir)
    conn.close()

    assert conn.closed


def test_connection_is_context_manager(tmpdir):
    with patch('lmdb.open') as lmdbopen:
        with Model.open(tmpdir) as conn:
            assert not conn.closed

            lmdbopen.assert_any_call(str(tmpdir) + "/readers", max_dbs=1)
            lmdbopen.assert_any_call(str(tmpdir) + "/data", max_dbs=2)

    assert conn.closed


@pytest.mark.parametrize("io_method", ["data",
                                       "readers",
                                       "create",
                                       "bulk_create",
                                       "reader",
                                       "register_reader",
                                       "unregister_reader",
                                       "save_registry",
                                       "list_readers",
                                       "remove",
                                       "purge"])
def test_cannot_use_same_connection_from_multiple_threads(tmpdir, io_method):
    conn = Model.open(tmpdir)

    with ThreadPoolExecutor(max_workers=1) as pool:
        res = pool.submit(getattr(conn, io_method))

    with pytest.raises(BadUsageError):
        res.result()


def test_connection_cannot_be_picklelized(tmpdir):
    conn = Model.open(tmpdir)

    with pytest.raises(BadUsageError):
        pickle.dumps(conn)


@pytest.mark.skip(reason="http://bugs.python.org/issue30549#")
@pytest.mark.parametrize("io_method", ["data",
                                       "readers",
                                       "create",
                                       "bulk_create",
                                       "reader",
                                       "register_reader",
                                       "unregister_reader",
                                       "save_registry",
                                       "list_readers",
                                       "remove",
                                       "purge"])
def test_cannot_use_same_connection_from_multiple_processes(tmpdir, io_method):
    conn = Model.open(tmpdir)

    with ProcessPoolExecutor(max_workers=2) as pool:
        with pytest.raises(BadUsageError):
            res = pool.submit(getattr(conn, io_method))



@pytest.mark.parametrize("io_method", ["data",
                                       "readers",
                                       "create",
                                       "bulk_create",
                                       "reader",
                                       "register_reader",
                                       "unregister_reader",
                                       "save_registry",
                                       "list_readers",
                                       "remove",
                                       "purge"])
def test_cannot_use_same_connection_after_when_is_closed(tmpdir, io_method):
    with Model.open(tmpdir) as db:
        pass

    with pytest.raises(BadUsageError):
        getattr(db, io_method)()
