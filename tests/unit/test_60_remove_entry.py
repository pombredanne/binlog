import os

import pytest
import lmdb

from binlog.model import Model
from binlog.index import TextIndex
from binlog.exceptions import ReaderDoesNotExist


def test_cant_remove_if_no_reader_exist(tmpdir):
    with Model.open(tmpdir) as db:
        e0 = db.create(test='data0')

        with pytest.raises(ReaderDoesNotExist):
            db.remove(e0)


def test_cant_remove_if_is_not_acked(tmpdir):
    with Model.open(tmpdir) as db:
        e0 = db.create(test='data0')

        db.register_reader('reader1')

        assert db.remove(e0) is False


def test_can_remove_if_is_acked(tmpdir):
    with Model.open(tmpdir) as db:
        e0 = db.create(test='data0')

        db.register_reader('reader1')
        with db.reader('reader1') as reader:
            reader.ack(e0)

        assert db.remove(e0) is True


def test_cant_remove_if_one_reader_is_not_acked(tmpdir):
    with Model.open(tmpdir) as db:
        e0 = db.create(test='data0')

        db.register_reader('reader1')
        with db.reader('reader1') as reader1:
            reader1.ack(e0)

        db.register_reader('reader2')

        assert db.remove(e0) is False


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


def test_remove_also_remove_index_entries(tmpdir):
    class CustomModel(Model):
        name = TextIndex()

    with CustomModel.open(tmpdir) as db:
        e0 = db.create(name="test1", idx=0)
        e1 = db.create(name="test1", idx=1)

        db.register_reader("reader")
        with db.reader("reader") as reader:
            reader.ack(e0)

        assert db.remove(e0)

    path = os.path.join(str(tmpdir), CustomModel._meta["data_env_directory"])
    env = lmdb.open(path, max_dbs=3)
    with env.begin() as txn:
        index_template = Model._meta["index_db_format"]
        index_db = env.open_db(
            index_template.format(model=CustomModel,
                                  index_name='name').encode('utf-8'),
            txn=txn)
        with txn.cursor(index_db) as cursor:
            cursor.set_key(b'test1')
            assert cursor.count() == 1


def test_remove_unsuccess(tmpdir):
    with Model.open(tmpdir) as db:
        e0 = db.create(test='data0')

        db.register_reader('reader1')
        with db.reader('reader1') as reader1:
            reader1.ack(e0)

        db.remove(e0)
        assert not db.remove(e0)
