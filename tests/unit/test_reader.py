from tempfile import mktemp
import shutil

import pytest

from binlog import reader, writer


#
# Reader
#
def test_Reader_exists():
    """The Reader exists."""
    assert hasattr(reader, 'Reader')

#
# Reader().next
#
def test_Reader_next_only_one_db():
    """
    Reader.next must return all entries in the log.
    """
    try:
        tmpdir = mktemp()

        # Write 10 entries
        w = writer.Writer(tmpdir, max_log_events=100)
        for i in range(10):
            w.append(i)
        w.current_log.sync()

        # Read 10 entries
        r = reader.Reader(tmpdir)
        for i in range(10):
            data = r.next()
            assert i == data

        assert r.next() is None

    except:
        raise
    finally:
        shutil.rmtree(tmpdir)


def test_Reader_next_multiple_db():
    """
    Reader.next must return all entries in the log.
    """
    try:
        tmpdir = mktemp()

        # Write 10 entries
        w = writer.Writer(tmpdir, max_log_events=2)
        for i in range(10):
            w.append(i)
        w.current_log.sync()

        # Read 10 entries
        r = reader.Reader(tmpdir)
        for i in range(10):
            data = r.next()
            assert i == data

        assert r.next() is None

    except:
        raise
    finally:
        shutil.rmtree(tmpdir)


def test_Reader_next_only_one_db_with_writings():
    """
    Reader.next must return all entries in the log even if the entries
    are added after the reader reach the last entry.
    """
    try:
        tmpdir = mktemp()

        # Write 10 entries
        w = writer.Writer(tmpdir, max_log_events=100)
        for i in range(10):
            w.append(i)
        w.current_log.sync()

        # Read 10 entries
        r = reader.Reader(tmpdir)
        for i in range(10):
            data = r.next()
            assert i == data

        assert r.next() is None

        for i in range(10, 20):
            w.append(i)
        w.current_log.sync()

        # Read 10 entries
        for i in range(10, 20):
            data = r.next()
            assert i == data

        assert r.next() is None
    except:
        raise
    finally:
        shutil.rmtree(tmpdir)
