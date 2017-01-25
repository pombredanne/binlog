from itertools import zip_longest

import pytest

from binlog.model import Model
from binlog.exceptions import ReaderDoesNotExist


def test_readers_environment_does_not_exist(tmpdir):
    with Model.open(tmpdir) as db:
        with pytest.raises(ReaderDoesNotExist):
            db.reader('nonexistingreader')


def test_non_existing_reader(tmpdir):
    with Model.open(tmpdir) as db:
        db.register_reader('new')

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


def test_unregister_unexisting_reader(tmpdir):
    with Model.open(tmpdir) as db:
        db.register_reader('reader')
        assert db.unregister_reader('reader')


def test_unregister_existing_reader(tmpdir):
    with Model.open(tmpdir) as db:
        with pytest.raises(ReaderDoesNotExist):
            db.unregister_reader('nonexistingreader')


def test_unregister_unexisting_reader_after_db_creation(tmpdir):
    with Model.open(tmpdir) as db:
        db.register_reader('otherreader')
        with pytest.raises(ReaderDoesNotExist):
            db.unregister_reader('nonexistingreader')


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


def test_reader_filter_exact(tmpdir):
    with Model.open(tmpdir) as db:
        entries = [Model(idx=i, even=(i % 2 == 0)) for i in range(100)]
        db.bulk_create(entries)

        with db.reader() as r:
            for a, b in zip_longest(r.filter(even=True), range(0, 100, 2)):
                assert a.pk == b


def test_reader_filter_exact_multiple(tmpdir):
    with Model.open(tmpdir) as db:
        entries = [Model(idx=i,
                         fizz=(i % 3 == 0),
                         buzz=(i % 5 == 0)) for i in range(100)]
        db.bulk_create(entries)

        with db.reader() as r:
            for a, b in zip_longest(r.filter(fizz=True, buzz=True),
                                    [x for x in range(0, 100)
                                     if x % 3 == x % 5 == 0]):
                assert a.pk == b
