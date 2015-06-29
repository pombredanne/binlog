from tempfile import mktemp
import shutil

import pytest


def test_reader_starts_with_the_lowest_database_available():
    from binlog.reader import Reader
    from binlog.writer import Writer

    try:
        tmpdir = mktemp()

        writer = Writer(tmpdir, max_log_events=10)

        for x in range(25):
            writer.append(x)
        writer.set_current_log().sync()

        writer.delete(1)

        reader = Reader(tmpdir, checkpoint='test')
        for x in range(10, 25):
            rec = reader.next_record()
            assert rec.value == x
        assert reader.next_record() is None
    except:
        raise
    finally:
        shutil.rmtree(tmpdir)
