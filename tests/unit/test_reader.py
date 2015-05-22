from tempfile import mktemp
import shutil

import pytest
import pickle

from binlog import reader, writer
from binlog.binlog import Record


#
# Reader
#
def test_Reader_exists():
    """The Reader exists."""
    assert hasattr(reader, 'Reader')


#
# Reader().next
#
def test_Reader_next():
    """The Reader has the next method."""
    assert hasattr(reader.Reader, 'next')


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
        w.set_current_log().sync()

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
        w.set_current_log().sync()

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
        w.set_current_log().sync()

        # Read 10 entries
        r = reader.Reader(tmpdir)
        for i in range(10):
            data = r.next()
            assert i == data

        assert r.next() is None

        for i in range(10, 20):
            w.append(i)
        w.set_current_log().sync()

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
        w.set_current_log().sync()

        # Read 10 entries
        r = reader.Reader(tmpdir)
        for i in range(10):
            data = r.next()
            assert i == data

        assert r.next() is None

        for i in range(10, 20):
            w.append(i)
        w.set_current_log().sync()

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
        w.set_current_log().sync()

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
# Reader().save
#
def test_Reader_save():
    """The Reader has the save method."""
    assert hasattr(reader.Reader, 'save')


def test_Reader_save_raises_if_no_checkpoint_defined():
    try:
        tmpdir = mktemp()

        r = reader.Reader(tmpdir)
        with pytest.raises(ValueError):
            r.save()
    except:
        raise
    finally:
        shutil.rmtree(tmpdir)


@pytest.mark.parametrize("max_log_events", range(1, 10))
def test_Reader_can_save_and_restore_its_process(max_log_events):
    """
    If the method checkpoint is called then a new instance must start
    with the first non-aknowledge item.

    """
    try:
        tmpdir = mktemp()

        # Write 10 entries
        w = writer.Writer(tmpdir, max_log_events=max_log_events)
        for i in range(10):
            w.append(i)
        w.set_current_log().sync()

        # Read first 5 entries
        r = reader.Reader(tmpdir, checkpoint='reader1')
        for i in range(5):
            data = r.next_record()
            r.ack(data)
            assert i == data.value
        r.save()  # Make a checkpoint

        # Read last 5 entries
        r = reader.Reader(tmpdir, checkpoint='reader1')
        for i in range(5, 10):
            data = r.next_record()
            assert i == data.value

        assert r.next_record() is None
    except:
        raise
    finally:
        shutil.rmtree(tmpdir)


@pytest.mark.parametrize("max_log_events", range(1, 10))
def test_Reader_can_save_and_restore_its_process_non_lineal(max_log_events):
    """
    If the method checkpoint is called then a new instance must start
    with the first non-aknowledge item. Even if there are gaps in the
    sequence.
    """
    try:
        tmpdir = mktemp()

        # Write 10 entries
        w = writer.Writer(tmpdir, max_log_events=max_log_events)
        for i in range(100):
            w.append(i)
        w.set_current_log().sync()

        # Read first 5 entries
        r = reader.Reader(tmpdir, checkpoint='reader1')
        for i in range(100):
            data = r.next_record()
            if i % 2 == 0:
                r.ack(data)
        r.save()  # Make a checkpoint

        # Read last 5 entries
        r = reader.Reader(tmpdir, checkpoint='reader1')
        for i in range(50):
            data = r.next_record()
            assert data.value % 2 != 0

        assert r.next_record() is None
    except:
        raise
    finally:
        shutil.rmtree(tmpdir)

#
# Reader().next_record
#
def test_Reader_next_record():
    """The Reader has the next_record method."""
    assert hasattr(reader.Reader, 'next_record')


@pytest.mark.parametrize("max_log_events", range(1, 10))
def test_Reader_next_record(max_log_events):
    """
    The next_record method retrieve the next register and return a Record
    object.
    """
    try:
        tmpdir = mktemp()

        # Write 10 entries
        w = writer.Writer(tmpdir, max_log_events=max_log_events)
        for i in range(10):
            w.append(i)
        w.set_current_log().sync()

        # Read first 5 entries
        r = reader.Reader(tmpdir)
        for i in range(10):
            data = r.next_record().value
            assert i == data

        assert r.next_record() is None
    except:
        raise
    finally:
        shutil.rmtree(tmpdir)


#
# Reader().ack
#
def test_Reader_ack():
    """The Reader has the ack method."""
    assert hasattr(reader.Reader, 'ack')


def test_Reader_register():
    """The Reader() has the register attribute."""
    try:
        tmpdir = mktemp()
        assert hasattr(reader.Reader(tmpdir), 'register')
    except:
        raise
    finally:
        shutil.rmtree(tmpdir)


@pytest.mark.parametrize("max_log_events", range(1, 10))
def test_Reader_ack_adds_to_register(max_log_events):
    """
    When some data retrieved from the next_record method is passed to
    the ack method, the Reader must add this to its register.
    """
    try:
        tmpdir = mktemp()

        # Write 10 entries
        w = writer.Writer(tmpdir, max_log_events=max_log_events)
        for i in range(10):
            w.append(i)
        w.set_current_log().sync()

        # Read first 5 entries
        r = reader.Reader(tmpdir)
        for i in range(10):
            data = r.next_record()
            assert data not in r.register
            r.ack(data)
            assert data in r.register

        assert r.next_record() is None
    except:
        raise
    finally:
        shutil.rmtree(tmpdir)


#
# Reader().has_next_log
#
def test_Reader_has_next_log():
    """The Reader has the has_next_log method."""
    assert hasattr(reader.Reader, 'has_next_log')


def test_Reader_has_next_log_one_log():
    """When there is no next log `has_next_log` must return `False`."""
    try:
        tmpdir = mktemp()

        # Write 10 entries
        w = writer.Writer(tmpdir)
        w.append('DATA')
        w.set_current_log().sync()

        # Read first 5 entries
        r = reader.Reader(tmpdir)
        r.next_record()

        assert not r.has_next_log()
    except:
        raise
    finally:
        shutil.rmtree(tmpdir)


def test_Reader_has_next_log_two_logs():
    """When there is next log `has_next_log` must return `True`."""
    try:
        tmpdir = mktemp()

        w = writer.Writer(tmpdir, max_log_events=1)
        w.append('DATA')
        w.append('DATA')
        w.set_current_log().sync()

        r = reader.Reader(tmpdir)
        r.next_record()

        assert r.has_next_log()
    except:
        raise
    finally:
        shutil.rmtree(tmpdir)

#
# Reader().set_cursors
#
def test_Reader_set_cursors():
    """The Reader has the set_cursors method."""
    assert hasattr(reader.Reader, 'set_cursors')


def test_Reader_set_cursors_from_record():
    """
    When set_cursors is called with a Record object li_cursor and
    cl_cursor must be pointing to the `liidx` and `clidx` attributesof
    the record.
    """
    try:
        tmpdir = mktemp()

        w = writer.Writer(tmpdir, max_log_events=1)
        w.append('DATA1')
        w.append('DATA2')
        w.set_current_log().sync()

        r = reader.Reader(tmpdir)

        r.set_cursors(Record(liidx=1, clidx=1, value=None))
        assert r.cl_cursor.current() == (1, pickle.dumps('DATA1'))

        r.set_cursors(Record(liidx=2, clidx=1, value=None))
        assert r.cl_cursor.current() == (1, pickle.dumps('DATA2'))

        r.set_cursors(Record(liidx=1, clidx=1, value=None))
        assert r.cl_cursor.current() == (1, pickle.dumps('DATA1'))

    except:
        raise
    finally:
        shutil.rmtree(tmpdir)
