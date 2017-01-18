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
    conn = CustomModel.open(tmpdir, **kwargs)

    assert conn.model is CustomModel
    assert conn.path == tmpdir
    assert conn.kwargs == kwargs
    assert not conn.closed


def test_connection_close(tmpdir):
    conn = Model.open(tmpdir)
    conn.close()

    assert conn.closed


@pytest.mark.wip
def test_connection_is_context_manager(tmpdir):
    with Model.open(tmpdir) as conn:
        assert not conn.closed

    assert not conn.closed
