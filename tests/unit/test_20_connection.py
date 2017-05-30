from unittest.mock import patch

import pytest

from binlog.model import Model


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
        lmdbopen.assert_any_call(str(tmpdir) + "/readers", max_dbs=1, **kwargs)
        lmdbopen.assert_any_call(str(tmpdir) + "/data", max_dbs=2, **kwargs)


def test_connection_close(tmpdir):
    conn = Model.open(tmpdir)
    conn.close()

    assert conn.closed


def test_connection_is_context_manager(tmpdir):
    with Model.open(tmpdir) as conn:
        assert not conn.closed

    assert conn.closed
