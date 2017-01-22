import pytest

from binlog.model import Model
from binlog.exceptions import ReaderDoesNotExist


@pytest.mark.wip
def test_cant_remove_if_no_reader_exist(tmpdir):
    with Model.open(tmpdir) as db:
        e0 = db.create(test='data0')

        with pytest.raises(ReaderDoesNotExist):
            db.remove(e0)


@pytest.mark.wip
def test_cant_remove_if_is_not_acked(tmpdir):
    with Model.open(tmpdir) as db:
        e0 = db.create(test='data0')

        db.register_reader('reader1')

        assert db.remove(e0) is False


@pytest.mark.wip
def test_can_remove_if_is_acked(tmpdir):
    with Model.open(tmpdir) as db:
        e0 = db.create(test='data0')

        db.register_reader('reader1')
        with db.reader('reader1') as reader:
            reader.ack(e0)

        assert db.remove(e0) is True


@pytest.mark.wip
def test_cant_remove_if_one_reader_is_not_acked(tmpdir):
    with Model.open(tmpdir) as db:
        e0 = db.create(test='data0')

        db.register_reader('reader1')
        with db.reader('reader1') as reader1:
            reader1.ack(e0)

        db.register_reader('reader2')

        assert db.remove(e0) is False


@pytest.mark.wip
def test_can_remove_if_all_readers_are_acked(tmpdir):
    with Model.open(tmpdir) as db:
        e0 = db.create(test='data0')

        db.register_reader('reader1')
        with db.reader('reader1') as reader1:
            reader1.ack(e0)

        db.register_reader('reader2')
        with db.reader('reader2') as reader2:
            reader2.ack(e0)

        assert db.remove(e0) is True


@pytest.mark.wip
def test_remove_delete_entry(tmpdir):
    with Model.open(tmpdir) as db:
        e0 = db.create(test='data0')

        db.register_reader('reader1')
        with db.reader('reader1') as reader1:
            reader1.ack(e0)

        db.remove(e0)

        db.register_reader('reader2')
        with db.reader('reader2') as reader2:
            assert not list(reader2)
