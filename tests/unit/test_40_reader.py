from itertools import zip_longest

import pytest

from binlog.model import Model
from binlog.exceptions import ReaderDoesNotExist


def test_non_existing_reader(tmpdir):
    with Model.open(tmpdir) as db:
        with pytest.raises(ReaderDoesNotExist):
            db.reader('nonexistingreader')


def test_register_reader(tmpdir):
    with Model.open(tmpdir) as db:
        assert db.register_reader('nonexistingreader')


def test_already_registered_reader(tmpdir):
    with Model.open(tmpdir) as db:
        db.register_reader('nonexistingreader')
        assert not db.register_reader('nonexistingreader')


def test_registered_reader_does_not_raise(tmpdir):
    with Model.open(tmpdir) as db:
        db.register_reader('readername')
        reader = db.reader('readername')
        assert reader


def test_reader_is_not_closed(tmpdir):
    with Model.open(tmpdir) as db:
        db.register_reader('readername')
        reader = db.reader('readername')
        assert not reader.closed


def test_reader_close(tmpdir):
    with Model.open(tmpdir) as db:
        db.register_reader('readername')
        reader = db.reader('readername')
        reader.close()
        assert reader.closed


def test_context_manager_closing(tmpdir):
    with Model.open(tmpdir) as db:
        db.register_reader('readername')
        with db.reader('readername') as reader:
            assert not reader.closed
        assert reader.closed


def test_reader_reads(tmpdir):
    with Model.open(tmpdir) as db:
        entries = [Model(idx=i) for i in range(10)]
        db.bulk_create(entries)

        db.register_reader('myreader')
        with db.reader('myreader') as reader:
            for current, expected in zip_longest(entries, reader):
                assert current == expected


def test_reader_reversed_read(tmpdir):
    with Model.open(tmpdir) as db:
        entries = [Model(idx=i) for i in range(10)]
        db.bulk_create(entries)

        db.register_reader('myreader')
        with db.reader('myreader') as reader:
            for current, expected in zip_longest(reversed(entries),
                                                 reversed(reader)):
                assert current == expected


def test_reader_index(tmpdir):
    with Model.open(tmpdir) as db:
        entries = [Model(idx=i) for i in range(10)]
        db.bulk_create(entries)

        db.register_reader('myreader')
        with db.reader('myreader') as reader:
            for e in entries:
                assert e == reader[e['idx']]


def test_reader_unknown_index(tmpdir):
    with Model.open(tmpdir) as db:
        db.create(test='test')

        db.register_reader('myreader')
        with db.reader('myreader') as reader:
            with pytest.raises(IndexError):
                reader[1000]


def test_ReadonlyError_is_masked(tmpdir):
    with Model.open(tmpdir) as db:
        db.register_reader('myreader')

        with db.reader('myreader') as reader:
            with pytest.raises(IndexError):
                reader[0]

            with pytest.raises(StopIteration):
                next(reader.__iter__())
            
            with pytest.raises(StopIteration):
                next(reader.__reversed__())


def test_anonymous_reader_does_not_raise(tmpdir):
    with Model.open(tmpdir) as db:
        reader = db.reader()
