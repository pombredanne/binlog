from tempfile import mktemp
import pickle
import shutil

from hypothesis import given
from hypothesis import strategies as st
import pytest

from binlog import reader, writer
from binlog.binlog import Record

from conftest import RW_IMPL

#
# Reader
#
def test_Reader_exists():
    """The Reader class exists."""
    assert hasattr(reader, 'Reader')


#
# Reader().next
#
def test_Reader_next():
    """The Reader has the next method."""
    assert hasattr(reader.Reader, 'next')


@pytest.mark.parametrize("rcls,wcls", RW_IMPL)
def test_Reader_next_only_one_db(rcls, wcls):
    """
    Reader.next must return all entries in the log.
    """
    try:
        tmpdir = mktemp()

        # Write 10 entries
        w = wcls(tmpdir, max_log_events=100)
        for i in range(10):
            w.append(i)
        w.set_current_log().sync()

        # Read 10 entries
        r = rcls(tmpdir)
        for i in range(10):
            data = r.next()
            assert i == data

        assert r.next() is None

    except:
        raise
    finally:
        shutil.rmtree(tmpdir)


@pytest.mark.parametrize("rcls,wcls", RW_IMPL)
def test_Reader_next_multiple_db(rcls, wcls):
    """
    Reader.next must return all entries in the log.
    """
    try:
        tmpdir = mktemp()

        # Write 10 entries
        w = wcls(tmpdir, max_log_events=2)
        for i in range(10):
            w.append(i)
        w.set_current_log().sync()

        # Read 10 entries
        r = rcls(tmpdir)
        for i in range(10):
            data = r.next()
            assert i == data

        assert r.next() is None

    except:
        raise
    finally:
        shutil.rmtree(tmpdir)


@pytest.mark.parametrize("rcls,wcls", RW_IMPL)
def test_Reader_next_only_one_db_with_writings(rcls, wcls):
    """
    Reader.next must return all entries in the log even if the entries
    are added after the reader reach the last entry.
    """
    try:
        tmpdir = mktemp()

        # Write 10 entries
        w = wcls(tmpdir, max_log_events=100)
        for i in range(10):
            w.append(i)
        w.set_current_log().sync()

        # Read 10 entries
        r = rcls(tmpdir)
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


@pytest.mark.parametrize("rcls,wcls", RW_IMPL)
@pytest.mark.parametrize("max_log_events", range(9, 21))
def test_Reader_next_only_multiple_dbs_with_writings(rcls, wcls, max_log_events):
    """
    Reader.next must return all entries in the log even if the entries
    are added after the reader reach the last entry.
    """
    try:
        tmpdir = mktemp()

        # Write 10 entries
        w = wcls(tmpdir, max_log_events=max_log_events)
        for i in range(10):
            w.append(i)
        w.set_current_log().sync()

        # Read 10 entries
        r = rcls(tmpdir)
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


@pytest.mark.parametrize("rcls,wcls", RW_IMPL)
def test_Reader_new_Reader_starts_over(rcls, wcls):
    """
    A new instance of Reader must start the reading from the begining.
    """
    try:
        tmpdir = mktemp()

        # Write 10 entries
        w = wcls(tmpdir, max_log_events=100)
        for i in range(10):
            w.append(i)
        w.set_current_log().sync()

        # Read 10 entries
        r = rcls(tmpdir)
        for i in range(10):
            data = r.next()
            assert i == data

        assert r.next() is None

        # Read 10 entries
        r = rcls(tmpdir)
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


@pytest.mark.parametrize("rcls,wcls", RW_IMPL)
def test_Reader_save_raises_if_no_checkpoint_defined(rcls, wcls):
    try:
        tmpdir = mktemp()

        # Write 10 entries
        w = wcls(tmpdir)
        for i in range(10):
            w.append(i)
        w.set_current_log().sync()

        r = rcls(tmpdir)
        with pytest.raises(ValueError):
            r.save()
    except:
        raise
    finally:
        shutil.rmtree(tmpdir)


@pytest.mark.parametrize("rcls,wcls", RW_IMPL)
@pytest.mark.parametrize("max_log_events", range(1, 10))
def test_Reader_can_save_and_restore_its_process(rcls, wcls, max_log_events):
    """
    If the method checkpoint is called then a new instance must start
    with the first non-aknowledge item.

    """
    try:
        tmpdir = mktemp()

        # Write 10 entries
        w = wcls(tmpdir, max_log_events=max_log_events)
        for i in range(10):
            w.append(i)
        w.set_current_log().sync()

        # Read first 5 entries
        r = rcls(tmpdir, checkpoint='reader1')
        for i in range(5):
            data = r.next_record()
            r.ack(data)
            assert i == data.value
        r.save()  # Make a checkpoint

        # Read last 5 entries
        r = rcls(tmpdir, checkpoint='reader1')
        for i in range(5, 10):
            data = r.next_record()
            assert i == data.value

        assert r.next_record() is None
    except:
        raise
    finally:
        shutil.rmtree(tmpdir)


@pytest.mark.parametrize("rcls,wcls", RW_IMPL)
@pytest.mark.parametrize("max_log_events", range(1, 10))
def test_Reader_can_save_and_restore_its_process_multiple_save(rcls, wcls, max_log_events):
    """
    If the method checkpoint is called then a new instance must start
    with the first non-aknowledge item.

    """
    try:
        tmpdir = mktemp()

        # Write 10 entries
        w = wcls(tmpdir, max_log_events=max_log_events)
        for i in range(10):
            w.append(i)
        w.set_current_log().sync()

        # Read first 5 entries
        r = rcls(tmpdir, checkpoint='reader1')
        for i in range(5):
            data = r.next_record()
            r.ack(data)
            assert i == data.value
            r.save()  # Make a checkpoint

        # Read last 5 entries
        r = rcls(tmpdir, checkpoint='reader1')
        for i in range(5, 10):
            data = r.next_record()
            assert i == data.value

        assert r.next_record() is None
    except:
        raise
    finally:
        shutil.rmtree(tmpdir)


@pytest.mark.parametrize("rcls,wcls", RW_IMPL)
@pytest.mark.parametrize("max_log_events", range(1, 10))
def test_Reader_can_save_and_restore_its_process_non_lineal(rcls, wcls, max_log_events):
    """
    If the method checkpoint is called then a new instance must start
    with the first non-aknowledge item. Even if there are gaps in the
    sequence.
    """
    try:
        tmpdir = mktemp()

        # Write 10 entries
        w = wcls(tmpdir, max_log_events=max_log_events)
        for i in range(100):
            w.append(i)
        w.set_current_log().sync()

        # Read first 5 entries
        r = rcls(tmpdir, checkpoint='reader1')
        for i in range(100):
            data = r.next_record()
            if i % 2 == 0:
                r.ack(data)
        r.save()  # Make a checkpoint

        # Read last 5 entries
        r = rcls(tmpdir, checkpoint='reader1')
        for i in range(50):
            data = r.next_record()
            assert data.value % 2 != 0

        assert r.next_record() is None
    except:
        raise
    finally:
        shutil.rmtree(tmpdir)

#
# Reader().load
#
def test_Reader_load():
    """The Reader has the load method."""
    assert hasattr(reader.Reader, 'load')


@pytest.mark.parametrize("rcls,wcls", RW_IMPL)
def test_Reader_load_raises_if_no_checkpoint(rcls, wcls):
    try:
        tmpdir = mktemp()

        # Write 10 entries
        w = wcls(tmpdir)
        for i in range(10):
            w.append(i)
        w.set_current_log().sync()

        r = rcls(tmpdir)
        with pytest.raises(ValueError):
            r.load()
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


@pytest.mark.parametrize("rcls,wcls", RW_IMPL)
@pytest.mark.parametrize("max_log_events", range(1, 10))
def test_Reader_next_record(rcls, wcls, max_log_events):
    """
    The next_record method retrieve the next register and return a Record
    object.
    """
    try:
        tmpdir = mktemp()

        # Write 10 entries
        w = wcls(tmpdir, max_log_events=max_log_events)
        for i in range(10):
            w.append(i)
        w.set_current_log().sync()

        # Read first 5 entries
        r = rcls(tmpdir)
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
@pytest.mark.parametrize("rcls,wcls", RW_IMPL)
def test_Reader_ack(rcls, wcls):
    """The Reader has the ack method."""
    try:
        tmpdir = mktemp()

        # Write 10 entries
        w = wcls(tmpdir)
        for i in range(10):
            w.append(i)
        w.set_current_log().sync()

        assert hasattr(reader.Reader, 'ack')
    except:
        raise
    finally:
        shutil.rmtree(tmpdir)


@pytest.mark.parametrize("rcls,wcls", RW_IMPL)
def test_Reader_register(rcls, wcls):
    """The Reader() has the register attribute."""
    try:
        tmpdir = mktemp()

        # Write 10 entries
        w = wcls(tmpdir)
        for i in range(10):
            w.append(i)
        w.set_current_log().sync()

        assert hasattr(rcls(tmpdir), 'register')
    except:
        raise
    finally:
        shutil.rmtree(tmpdir)


@pytest.mark.parametrize("rcls,wcls", RW_IMPL)
@pytest.mark.parametrize("max_log_events", range(1, 10))
def test_Reader_ack_adds_to_register(rcls, wcls, max_log_events):
    """
    When some data retrieved from the next_record method is passed to
    the ack method, the Reader must add this to its register.
    """
    try:
        tmpdir = mktemp()

        # Write 10 entries
        w = wcls(tmpdir, max_log_events=max_log_events)
        for i in range(10):
            w.append(i)
        w.set_current_log().sync()

        # Read first 5 entries
        r = rcls(tmpdir)
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


@pytest.mark.parametrize("rcls,wcls", RW_IMPL)
def test_Reader_has_next_log_one_log(rcls, wcls):
    """When there is no next log `has_next_log` must return `False`."""
    try:
        tmpdir = mktemp()

        # Write 10 entries
        w = wcls(tmpdir)
        w.append('DATA')
        w.set_current_log().sync()

        # Read first 5 entries
        r = rcls(tmpdir)
        r.next_record()

        assert not r.has_next_log()
    except:
        raise
    finally:
        shutil.rmtree(tmpdir)


@pytest.mark.parametrize("rcls,wcls", RW_IMPL)
def test_Reader_has_next_log_two_logs(rcls, wcls):
    """When there is next log `has_next_log` must return `True`."""
    try:
        tmpdir = mktemp()

        w = wcls(tmpdir, max_log_events=1)
        w.append('DATA')
        w.append('DATA')
        w.set_current_log().sync()

        r = rcls(tmpdir)
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


@pytest.mark.parametrize("rcls,wcls", RW_IMPL)
def test_Reader_set_cursors_from_record(rcls, wcls):
    """
    When set_cursors is called with a Record object li_cursor and
    cl_cursor must be pointing to the `liidx` and `clidx` attributesof
    the record.
    """
    try:
        tmpdir = mktemp()

        w = wcls(tmpdir, max_log_events=1)
        w.append('DATA1')
        w.append('DATA2')
        w.set_current_log().sync()

        r = rcls(tmpdir)

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

#
# Reader().status
#
def test_Reader_status():
    """The Reader class has the status method."""
    assert hasattr(reader.Reader, 'status')


@pytest.mark.parametrize("rcls,wcls", RW_IMPL)
def test_Reader_status_return_dict(rcls, wcls):
    """The method Reader.status returns a dictionary."""
    try:
        tmpdir = mktemp()

        w = wcls(tmpdir, max_log_events=1)
        r = rcls(tmpdir)
        assert type(r.status()) == dict
    except:
        raise
    finally:
        shutil.rmtree(tmpdir)


@pytest.mark.parametrize("rcls,wcls", RW_IMPL)
@given(num=st.integers(min_value=1, max_value=100))
def test_Reader_status_return_dict_with_status(rcls, wcls, num):
    """
    The method Reader.status returns a dictionary with the indexes and
    the status of each index.
    """
    try:
        tmpdir = mktemp()

        w = wcls(tmpdir, max_log_events=10)
        r = rcls(tmpdir)

        for i in range(num):
            w.append(i)

        w.set_current_log().sync()
        assert not any(r.status().values())

        for i in range(num):
            data = r.next_record()
            r.ack(data)

        assert all(r.status().values())
    except:
        raise
    finally:
        shutil.rmtree(tmpdir)

#
# No database exceptions
#
@pytest.mark.parametrize("rcls,wcls", RW_IMPL)
def test_no_environment(rcls, wcls):
    tmpdir = mktemp()
    with pytest.raises(ValueError):
        rcls(tmpdir)


@pytest.mark.parametrize("rcls,wcls", RW_IMPL)
def test_environment_but_no_databases(rcls, wcls):
    try:
        tmpdir = mktemp()
        w = wcls(tmpdir)
        r = rcls(tmpdir)
        assert r.next_record() is None
    except:
        raise
    finally:
        shutil.rmtree(tmpdir)


@pytest.mark.parametrize("rcls,wcls", RW_IMPL)
def test_environment_but_no_databases_and_after_read_is_fine(rcls, wcls):
    try:
        tmpdir = mktemp()
        w = wcls(tmpdir)
        r = rcls(tmpdir)
        assert r.next_record() is None

        w.append('TEST')
        w.set_current_log().sync()

        assert r.next_record() is not None
    except:
        raise
    finally:
        shutil.rmtree(tmpdir)


@pytest.mark.parametrize("rcls,wcls", RW_IMPL)
def test_environment_but_no_databases_and_after_read_is_fine_multiple_retry(rcls, wcls):
    try:
        tmpdir = mktemp()
        w = wcls(tmpdir)
        r = rcls(tmpdir)

        for i in range(10):
            assert r.next_record() is None

        w.append('TEST')
        w.set_current_log().sync()

        assert r.next_record() is not None
    except:
        raise
    finally:
        shutil.rmtree(tmpdir)
