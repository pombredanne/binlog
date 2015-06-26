import shutil
from tempfile import mktemp

import pytest

MAX_LOG_EVENTS = 10


@pytest.mark.parametrize("reads", range(1, MAX_LOG_EVENTS * 2 + 2))
def test_status_and_delete_match(reads):
    """reader.status is consistent with writer.delete."""
    from binlog.reader import Reader
    from binlog.writer import Writer
    try:
        tmpdir = mktemp()

        writer = Writer(tmpdir, max_log_events=MAX_LOG_EVENTS)
        reader = Reader(tmpdir, checkpoint='test')

        for x in range(25):
            writer.append(x)

        for x in range(reads):
            rec = reader.next_record()
            reader.ack(rec)
        reader.save()

        status = reader.status() 

        for idx, can_delete in status.items():
            if idx == max(status.keys()):
                # We cannot delete the last DB cause is current.
                assert not can_delete
                with pytest.raises(ValueError):
                    writer.delete(idx)
            else:
                if can_delete:
                    writer.delete(idx)
    except:
        raise
    finally:
        shutil.rmtree(tmpdir)


def test_status_method_works_after_deletion():
    from binlog.reader import Reader
    from binlog.writer import Writer

    try:
        tmpdir = mktemp()

        writer = Writer(tmpdir, max_log_events=10)
        reader = Reader(tmpdir, checkpoint='test')

        for x in range(25):
            writer.append(x)

        reader.status()
        writer.delete(1)
        reader.status()
    except:
        raise
    finally:
        shutil.rmtree(tmpdir)
