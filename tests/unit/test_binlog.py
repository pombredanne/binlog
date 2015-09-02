from tempfile import mktemp
from unittest.mock import patch
import os
import shutil

from bsddb3.db import DBEnv, DB
import pytest

from binlog import binlog

from conftest import BINLOG_IMPL

#
# Binlog
#
def test_Binlog_exists():
    """The Binlog exists."""
    assert hasattr(binlog, 'Binlog')


#
# Binlog.flags
#
def test_Binlog_flags_exists():
    """The Binlog base class must have an .flags attribute."""
    assert hasattr(binlog.Binlog, 'flags')


def test_Binlog_flags_is_empty():
    """The Binlog base class must have an empty .flags attribute."""
    assert binlog.Binlog.flags is None

#
# Binlog.open_environ
#
def test_Binlog_open_environ():
    """The Binlog has the open_environ method."""
    assert hasattr(binlog.Binlog, 'open_environ')


def test_Binlog_open_environ_abstract():
    """
    Binlog has no flags.
    The open_environ method MUST raise NotImplementedError in this case.

    """
    with pytest.raises(NotImplementedError):
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

#
# Test implementations...
#

@pytest.mark.parametrize("cls", BINLOG_IMPL)
def test_Binlog_open_environ_new(cls):
    """If the environ does not exist then it must be created."""
    try:
        tmpdir = mktemp()

        assert not os.path.isdir(tmpdir)
        env = cls.open_environ(tmpdir)
        assert os.path.isdir(tmpdir)
        assert isinstance(env, DBEnv().__class__)
    except:
        raise
    finally:
        shutil.rmtree(tmpdir)


@pytest.mark.parametrize("cls", BINLOG_IMPL)
def test_Binlog_open_environ_but_is_file(cls):
    """If the path passed is a file, then the environ can't be created."""
    try:
        tmpdir = mktemp()

        open(tmpdir, 'w').close()
        assert os.path.isfile(tmpdir)

        with pytest.raises(ValueError):
            cls.open_environ(tmpdir)
    except:
        raise
    finally:
        os.unlink(tmpdir)


@pytest.mark.parametrize("cls", BINLOG_IMPL)
def test_Binlog_open_environ_but_exists(cls):
    """If the environ already exist, then only open it."""
    try:
        tmpdir = mktemp()

        os.makedirs(tmpdir)
        assert os.path.isdir(tmpdir)

        with patch('os.makedirs') as mock:
            mock.return_value = None
            env = cls.open_environ(tmpdir)
            assert not mock.called

        assert isinstance(env, DBEnv().__class__)
    except:
        raise
    finally:
        shutil.rmtree(tmpdir)

#
# Binlog.open_logindex
#
@pytest.mark.parametrize("cls", BINLOG_IMPL)
def test_Binlog_open_logindex(cls):
    """The Binlog has the open_logindex method."""
    assert hasattr(cls, 'open_logindex')


@pytest.mark.parametrize("cls", BINLOG_IMPL)
def test_Binlog_open_logindex_new(cls):
    """If the logindex does not exist then it must be created."""
    try:
        tmpdir = mktemp()
        logfile = os.path.join(tmpdir, 'logindex')

        assert not os.path.isfile(logfile)

        logindex = cls.open_logindex(
            cls.open_environ(tmpdir),
            'logindex')

        assert os.path.isfile(logfile)
        assert isinstance(logindex, DB().__class__)
    except:
        raise
    finally:
        shutil.rmtree(tmpdir)


@pytest.mark.parametrize("cls", BINLOG_IMPL)
def test_Binlog_open_logindex_but_is_dir(cls):
    """If the path passed is a file, then the environ can't be created."""
    try:
        tmpdir = mktemp()
        logfile = os.path.join(tmpdir, 'logindex')

        env = cls.open_environ(tmpdir)

        os.makedirs(logfile)
        assert os.path.isdir(logfile)

        with pytest.raises(ValueError):
            logindex = cls.open_logindex(env, 'logindex')
    except:
        raise
    finally:
        shutil.rmtree(tmpdir)
