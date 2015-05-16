from tempfile import mktemp
from unittest.mock import patch
import os
import shutil

from bsddb3.db import DBEnv
import pytest

from binlog import writer


def test_Writer_exists():
    """The Writer exists."""
    assert hasattr(writer, 'Writer')


def test_Writer_create_environ():
    """The Writer has the create_environ method."""
    assert hasattr(writer.Writer, 'create_environ')


def test_Writer_create_environ_new():
    """If the environ do not exists then it must be created."""
    try:
        tmpdir = mktemp()

        assert not os.path.isdir(tmpdir)
        env = writer.Writer.create_environ(tmpdir)
        assert os.path.isdir(tmpdir)
        assert isinstance(env, DBEnv().__class__)
    except:
        raise
    finally:
        try:
            shutil.rmtree(tmpdir)
        except:
            pass


def test_Writer_create_environ_but_is_file():
    """If the path passed is a file, then the environ can't be created."""
    try:
        tmpdir = mktemp()

        open(tmpdir, 'w').close()
        assert os.path.isfile(tmpdir)

        with pytest.raises(ValueError):
            writer.Writer.create_environ(tmpdir)
    except:
        raise
    finally:
        try:
            os.unlink(tmpdir)
        except:
            pass



def test_Writer_create_environ_but_exists():
    """If the environ already exist, then only open it."""
    try:
        tmpdir = mktemp()

        os.makedirs(tmpdir)
        assert os.path.isdir(tmpdir)

        with patch('os.makedirs') as mock:
            mock.return_value = None
            env = writer.Writer.create_environ(tmpdir)
            assert not mock.called

        assert isinstance(env, DBEnv().__class__)
    except:
        raise
    finally:
        try:
            shutil.rmtree(tmpdir)
        except:
            pass
