from unittest.mock import MagicMock

import pytest

from binlog.connectionmanager import ConnectionManager
from binlog.model import Model


def test_connectionmanager_instance_connectionclass():
    c = ConnectionManager()
    connection_class = MagicMock()

    path = '/some/path'
    kwargs = {'key1': 'value1',
              'key2': 'value2'}

    c.open(model=Model,
           path=path,
           connection_class=connection_class,
           kwargs=kwargs)

    connection_class.assert_called_once_with(
        model=Model, path=path, kwargs=kwargs)


def test_connectionmanager_return_instance():
    c = ConnectionManager()
    connection_instance = MagicMock()
    connection_instance.open.return_value = 'MYCONNECTION'
    connection_class = MagicMock(return_value=connection_instance)

    path = '/some/path'
    kwargs = {'key1': 'value1',
              'key2': 'value2'}

    assert c.open(model=Model,
                  path=path,
                  connection_class=connection_class,
                  kwargs=kwargs) == 'MYCONNECTION'

    assert connection_instance.open.called


def test_connectionmanager_same_args_twice_dont_instance_twice():
    c = ConnectionManager()
    connection_class = MagicMock()

    path = '/some/path'
    kwargs = {'key1': 'value1',
              'key2': 'value2'}

    c.open(model=Model,
           path=path,
           connection_class=connection_class,
           kwargs=kwargs)

    c.open(model=Model,
           path=path,
           connection_class=connection_class,
           kwargs=kwargs)

    connection_class.assert_called_once_with(
        model=Model, path=path, kwargs=kwargs)


def test_connectionmanager_same_path_different_args_raise():

    c = ConnectionManager()
    connection_class = MagicMock()

    path = '/some/path'
    kwargs = {'key1': 'value1',
              'key2': 'value2'}

    c.open(model=Model,
           path=path,
           connection_class=connection_class,
           kwargs=kwargs)

    kwargs['key3'] = 'value3'
    with pytest.raises(ValueError):
        c.open(model=Model,
               path=path,
               connection_class=connection_class,
               kwargs=kwargs)


def test_connectionmanager_is_connected():

    c = ConnectionManager()
    connection_class = MagicMock()

    path = '/some/path'
    kwargs = {'key1': 'value1',
              'key2': 'value2'}

    c.open(model=Model,
           path=path,
           connection_class=connection_class,
           kwargs=kwargs)

    kwargs['key3'] = 'value3'
    with pytest.raises(ValueError):
        c.open(model=Model,
               path=path,
               connection_class=connection_class,
               kwargs=kwargs)


def test_reset_connections():
    from binlog.connectionmanager import reset_connections, PROCESS_CONNECTIONS
    old_process_connections = PROCESS_CONNECTIONS

    reset_connections()

    from binlog.connectionmanager import reset_connections, PROCESS_CONNECTIONS
    assert old_process_connections is not PROCESS_CONNECTIONS
