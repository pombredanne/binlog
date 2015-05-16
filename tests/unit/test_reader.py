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


@pytest.mark.parametrize("max_log_events", range(9, 21))
def test_Reader_next_only_multiple_dbs_with_writings(max_log_events):
    """
    Reader.next must return all entries in the log even if the entries
    are added after the reader reach the last entry.
    """
    try:
        tmpdir = mktemp()

        # Write 10 entries
        w = writer.Writer(tmpdir, max_log_events=max_log_events)
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


def test_Reader_new_Reader_starts_over():
    """
    A new instance of Reader must start the reading from the begining.
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


#
# Reader().checkpoint
#
@pytest.mark.parametrize("max_log_events", range(1, 10))
def test_Reader_can_save_and_restore_its_process(max_log_events):
    """
    If the method checkpoint is called then a new instance must start at
    the same point.
    """
    try:
        tmpdir = mktemp()

        # Write 10 entries
        w = writer.Writer(tmpdir, max_log_events=max_log_events)
        for i in range(10):
            w.append(i)
        w.current_log.sync()

        # Read first 5 entries
        r = reader.Reader(tmpdir, checkpoint='reader1')
        for i in range(5):
            data = r.next()
            assert i == data
        r.save()  # Make a checkpoint

        # Read last 5 entries
        r = reader.Reader(tmpdir, checkpoint='reader1')
        for i in range(5, 10):
            data = r.next()
            assert i == data

        assert r.next() is None
    except:
        raise
    finally:
        shutil.rmtree(tmpdir)
