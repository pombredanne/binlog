from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from unittest.mock import patch
import pickle
import time
import os

import pytest

from binlog.model import Model
from binlog.exceptions import BadUsageError

io_methods = ["data", "readers", "create", "bulk_create", "reader",
              "register_reader", "unregister_reader", "save_registry", "list_readers",
              "remove", "purge"]

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

        assert lmdbopen.called, "Should be called on first open"


def test_connection_close(tmpdir):
    conn = Model.open(tmpdir)
    conn.close()

    assert conn.closed


@pytest.mark.wip
def test_connection_is_context_manager(tmpdir):
    with patch('lmdb.open') as lmdbopen:
        with Model.open(tmpdir) as conn:
            assert not conn.closed

            lmdbopen.assert_any_call(str(tmpdir) + "/readers", max_dbs=1)
            lmdbopen.assert_any_call(str(tmpdir) + "/data", max_dbs=2)

    assert conn.closed


@pytest.mark.parametrize("io_method", io_methods)
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
@pytest.mark.parametrize("io_method", io_methods)
def test_cannot_use_same_connection_from_multiple_processes(tmpdir, io_method):
    conn = Model.open(tmpdir)

    with ProcessPoolExecutor(max_workers=2) as pool:
        with pytest.raises(BadUsageError):
            res = pool.submit(getattr(conn, io_method))



@pytest.mark.parametrize("io_method", io_methods)
def test_cannot_use_same_connection_after_when_is_closed(tmpdir, io_method):
    with Model.open(tmpdir) as db:
        pass

    with pytest.raises(BadUsageError):
        getattr(db, io_method)()


def test_same_database_is_same_connection(tmpdir):
    assert Model.open(tmpdir) is Model.open(tmpdir)


def test_too_many_close(tmpdir):
    conn1 = Model.open(tmpdir)
    conn1.close()
    with pytest.raises(BadUsageError):
        conn1.close()


def test_cant_reopen_a_closed_connection(tmpdir):
    conn1 = Model.open(tmpdir)
    conn1.close()
    assert conn1.closed
    with pytest.raises(BadUsageError):
        conn1.open()


@pytest.mark.parametrize("io_method", io_methods)
def test_connection_not_allowed_from_different_processes(tmpdir, io_method):
    r, w = os.pipe()
    os.set_inheritable(r, True)
    os.set_inheritable(w, True)

    with Model.open(tmpdir) as db:
        pid = os.fork()
        if pid != 0:  # Parent
            os.close(w)
            with os.fdopen(r, 'rb') as pipe:
                try:
                    getattr(db, io_method)()  # This should not raise BadUsageError
                except BadUsageError as exc:
                    assert False, "Unexpected exception in parent %r" % exc 
                except Exception as exc:
                    # Possible expected behavior because we are not calling
                    # every io_method with all the neded parameters
                    pass
                finally:
                    # Receive child result
                    result, message = pickle.load(pipe)
                    assert result, message
                    os.waitpid(pid, 0)
        else:  # Child
            os.close(r)
            with os.fdopen(w, 'wb') as pipe:
                try: 
                    getattr(db, io_method)()  # This should not raise BadUsageError
                except BadUsageError:
                    pickle.dump((True, "Expected behavior"), pipe)
                except Exception as exc:
                    # This is not expected even when we are calling the
                    # io_method with incorrect parameters because BadUsageError
                    # MUST raise first given that is in the decorator.
                    pickle.dump((False, "Unexpected exception in child %r" % exc), pipe)
                else:
                    pickle.dump((False, "Child did not raise"), pipe)
            os._exit(0)
