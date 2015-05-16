from tempfile import mktemp
from unittest.mock import patch
import os
import shutil

from bsddb3.db import DBEnv, DB
import pytest

from binlog import binlog

#
# Binlog
#
def test_Binlog_exists():
    """The Binlog exists."""
    assert hasattr(binlog, 'Binlog')

#
# Binlog.open_environ
#
def test_Binlog_open_environ():
    """The Binlog has the open_environ method."""
    assert hasattr(binlog.Binlog, 'open_environ')


def test_Binlog_open_environ_new():
    """If the environ does not exist then it must be created."""
    try:
        tmpdir = mktemp()

        assert not os.path.isdir(tmpdir)
        env = binlog.Binlog.open_environ(tmpdir)
        assert os.path.isdir(tmpdir)
        assert isinstance(env, DBEnv().__class__)
    except:
        raise
    finally:
        shutil.rmtree(tmpdir)


def test_Binlog_open_environ_but_is_file():
    """If the path passed is a file, then the environ can't be created."""
    try:
        tmpdir = mktemp()

        open(tmpdir, 'w').close()
        assert os.path.isfile(tmpdir)

        with pytest.raises(ValueError):
            binlog.Binlog.open_environ(tmpdir)
    except:
        raise
    finally:
        os.unlink(tmpdir)


def test_Binlog_open_environ_but_exists():
    """If the environ already exist, then only open it."""
    try:
        tmpdir = mktemp()

        os.makedirs(tmpdir)
        assert os.path.isdir(tmpdir)

        with patch('os.makedirs') as mock:
            mock.return_value = None
            env = binlog.Binlog.open_environ(tmpdir)
            assert not mock.called

        assert isinstance(env, DBEnv().__class__)
    except:
        raise
    finally:
        shutil.rmtree(tmpdir)

#
# Binlog.open_logindex
#
def test_Binlog_open_logindex():
    """The Binlog has the open_logindex method."""
    assert hasattr(binlog.Binlog, 'open_logindex')


def test_Binlog_open_logindex_new():
    """If the logindex does not exist then it must be created."""
    try:
        tmpdir = mktemp()
        logfile = os.path.join(tmpdir, 'logindex')

        assert not os.path.isfile(logfile)

        logindex = binlog.Binlog.open_logindex(
            binlog.Binlog.open_environ(tmpdir),
            'logindex')

        assert os.path.isfile(logfile)
        assert isinstance(logindex, DB().__class__)
    except:
        raise
    finally:
        shutil.rmtree(tmpdir)


def test_Binlog_open_logindex_but_is_dir():
    """If the path passed is a file, then the environ can't be created."""
    try:
        tmpdir = mktemp()
        logfile = os.path.join(tmpdir, 'logindex')

        env = binlog.Binlog.open_environ(tmpdir)

        os.makedirs(logfile)
        assert os.path.isdir(logfile)

        with pytest.raises(ValueError):
            logindex = binlog.Binlog.open_logindex(env, 'logindex')
    except:
        raise
    finally:
        shutil.rmtree(tmpdir)
