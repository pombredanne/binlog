from tempfile import mktemp
from unittest.mock import patch
import os
import shutil

from bsddb3.db import DBEnv, DB
import pytest

from binlog import writer


#
# Writer
#
def test_Writer_exists():
    """The Writer exists."""
    assert hasattr(writer, 'Writer')


#
# Writer.open_environ
#
def test_Writer_open_environ():
    """The Writer has the open_environ method."""
    assert hasattr(writer.Writer, 'open_environ')


def test_Writer_open_environ_new():
    """If the environ does not exist then it must be created."""
    try:
        tmpdir = mktemp()

        assert not os.path.isdir(tmpdir)
        env = writer.Writer.open_environ(tmpdir)
        assert os.path.isdir(tmpdir)
        assert isinstance(env, DBEnv().__class__)
    except:
        raise
    finally:
        try:
            shutil.rmtree(tmpdir)
        except:
            pass


def test_Writer_open_environ_but_is_file():
    """If the path passed is a file, then the environ can't be created."""
    try:
        tmpdir = mktemp()

        open(tmpdir, 'w').close()
        assert os.path.isfile(tmpdir)

        with pytest.raises(ValueError):
            writer.Writer.open_environ(tmpdir)
    except:
        raise
    finally:
        try:
            os.unlink(tmpdir)
        except:
            pass


def test_Writer_open_environ_but_exists():
    """If the environ already exist, then only open it."""
    try:
        tmpdir = mktemp()

        os.makedirs(tmpdir)
        assert os.path.isdir(tmpdir)

        with patch('os.makedirs') as mock:
            mock.return_value = None
            env = writer.Writer.open_environ(tmpdir)
            assert not mock.called

        assert isinstance(env, DBEnv().__class__)
    except:
        raise
    finally:
        try:
            shutil.rmtree(tmpdir)
        except:
            pass

#
# Writer.open_logindex
#
def test_Writer_open_logindex():
    """The Writer has the open_logindex method."""
    assert hasattr(writer.Writer, 'open_logindex')


def test_Writer_open_logindex_new():
    """If the logindex does not exist then it must be created."""
    try:
        tmpdir = mktemp()
        logfile = os.path.join(tmpdir, 'logindex')

        assert not os.path.isfile(logfile)

        logindex = writer.Writer.open_logindex(
            writer.Writer.open_environ(tmpdir),
            'logindex')

        assert os.path.isfile(logfile)
        assert isinstance(logindex, DB().__class__)
    except:
        raise
    finally:
        try:
            shutil.rmtree(tmpdir)
        except:
            pass


def test_Writer_open_logindex_but_is_dir():
    """If the path passed is a file, then the environ can't be created."""
    try:
        tmpdir = mktemp()
        logfile = os.path.join(tmpdir, 'logindex')

        env = writer.Writer.open_environ(tmpdir)

        os.makedirs(logfile)
        assert os.path.isdir(logfile)

        with pytest.raises(ValueError):
            logindex = writer.Writer.open_logindex(env, 'logindex')
    except:
        raise
    finally:
        try:
            os.unlink(tmpdir)
        except:
            pass


#
# Writer()
#
def test_Writer_instantiation():
    """
    At the moment of instantiation the environment and the logindex
    must be created.
    """
    try:
        tmpdir = mktemp()

        w = writer.Writer(tmpdir)
        assert os.path.isdir(tmpdir)
        assert os.path.isfile(os.path.join(tmpdir, writer.LOGINDEX_NAME))
        assert isinstance(w.env, DBEnv().__class__)
        assert isinstance(w.logindex, DB().__class__)
    except:
        raise
    finally:
        try:
            os.rmdir(tmpdir)
        except:
            pass


#
# Writer().current_log
#
def test_Writer_current_log():
    """The Writer has the open_environ method."""
    assert hasattr(writer.Writer, 'current_log')


def test_Writer_current_log_on_new_environ():
    """
    When a new environ/logindex is created, current_log must create a
    new log and this new log must be registered in the logindex.
    """
    try:
        tmpdir = mktemp()

        w = writer.Writer(tmpdir)
        cl = w.current_log

        assert os.path.isfile(os.path.join(tmpdir, writer.LOG_PREFIX + '.1'))
        assert isinstance(cl, DB().__class__)

        cursor = w.logindex.cursor()
        f_idx, f_name = cursor.first()
        l_idx, l_name = cursor.last()
        cursor.close()

        expected_idx = 1
        expected_name = (writer.LOG_PREFIX + '.1').encode('utf-8')

        assert f_idx == l_idx == expected_idx
        assert f_name == l_name ==  expected_name
    except:
        raise
    finally:
        try:
            os.rmdir(tmpdir)
        except:
            pass


def test_Writer_current_log_on_created_but_is_empty():
    """
    If the environ is initialized, current_log must return the already
    created log if it have space available.
    """
    try:
        tmpdir = mktemp()

        w = writer.Writer(tmpdir)  # This will create the environ
        cl = w.current_log         # This will create the first event DB
        del cl
        del w

        w = writer.Writer(tmpdir)
        cl = w.current_log

        assert os.path.isfile(os.path.join(tmpdir, writer.LOG_PREFIX + '.1'))
        assert isinstance(cl, DB().__class__)

        cursor = w.logindex.cursor()
        f_idx, f_name = cursor.first()
        l_idx, l_name = cursor.last()
        cursor.close()

        expected_idx = 1
        expected_name = (writer.LOG_PREFIX + '.1').encode('utf-8')

        assert f_idx == l_idx == expected_idx
        assert f_name == l_name ==  expected_name
    except:
        raise
    finally:
        try:
            os.rmdir(tmpdir)
        except:
            pass


def test_Writer_current_log_on_created_with_space():
    """
    If there is available space in the last log this must be returned.
    """
    try:
        tmpdir = mktemp()

        w = writer.Writer(tmpdir, max_log_events=10)  # This will create the environ
        cl = w.current_log         # This will create the first event DB

        for i in range(5):
            cl.append(b'data')
        cl.close()
        del cl
        del w

        w = writer.Writer(tmpdir, max_log_events=10)
        cl = w.current_log

        assert os.path.isfile(os.path.join(tmpdir, writer.LOG_PREFIX + '.1'))
        assert isinstance(cl, DB().__class__)

        cursor = w.logindex.cursor()
        f_idx, f_name = cursor.first()
        l_idx, l_name = cursor.last()
        cursor.close()

        expected_idx = 1
        expected_name = (writer.LOG_PREFIX + '.1').encode('utf-8')

        assert f_idx == l_idx == expected_idx
        assert f_name == l_name ==  expected_name
    except:
        raise
    finally:
        try:
            os.rmdir(tmpdir)
        except:
            pass


def test_Writer_current_log_on_created_but_full():
    """
    If there is available space in the last log this must be returned.
    """
    try:
        tmpdir = mktemp()

        w = writer.Writer(tmpdir, max_log_events=10)  # This will create
                                                      # the environ
        cl = w.current_log         # This will create the first event DB

        for i in range(10):
            cl.append(b'data')
        cl.close()
        del cl
        del w

        w = writer.Writer(tmpdir, max_log_events=10)
        cl = w.current_log

        assert os.path.isfile(os.path.join(tmpdir, writer.LOG_PREFIX + '.2'))
        assert isinstance(cl, DB().__class__)

        cursor = w.logindex.cursor()
        f_idx, f_name = cursor.first()
        l_idx, l_name = cursor.last()
        cursor.close()

        expected_idx = 2
        expected_name = (writer.LOG_PREFIX + '.2').encode('utf-8')

        assert f_idx != l_idx == expected_idx
        assert f_name != l_name ==  expected_name
    except:
        raise
    finally:
        try:
            os.rmdir(tmpdir)
        except:
            pass

#
# Writer().append
#
def test_Writer_append():
    """The Writer has the append method."""
    assert hasattr(writer.Writer, 'append')


def test_Writer_append_add_data_to_current_log():
    """
    When append() is called the data passed is pickelized and stored in
    the current log.
    """
    import pickle
    try:
        tmpdir = mktemp()

        expected = ["Some data", "Other data"]

        w = writer.Writer(tmpdir)
        w.append(expected)

        cl = w.current_log
        cursor = cl.cursor()
        idx, data = cursor.first()
        cursor.close()

        actual = pickle.loads(data)

        assert expected == actual

    except:
        raise
    finally:
        try:
            os.rmdir(tmpdir)
        except:
            pass


def test_Writer_multiple_appends_creates_multiple_log():
    """
    When append is called multiple times, if max_log_events is reached,
    then a new log DB must be created.
    """
    import pickle
    try:
        tmpdir = mktemp()

        w = writer.Writer(tmpdir, max_log_events=2)
        for i in range(20):
            w.append("TEST DATA")

        cursor = w.logindex.cursor()

        for i in range(1, 11):  # Assert we created 10 DBs
            idx, _ = cursor.next()
            assert idx == i

        assert cursor.next() is None  # And nothing more
        cursor.close()

    except:
        raise
    finally:
        try:
            os.rmdir(tmpdir)
        except:
            pass
