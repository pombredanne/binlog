import pickle
import struct

import lmdb
import pytest

from binlog.model import Model


def test_create_store_entry(tmpdir):
    saved = {'key1': 'value1', 'key2': 'value2'}

    with Model.open(tmpdir) as db:
        entry = db.create(**saved)

    env = lmdb.open(str(tmpdir), max_dbs=1)
    with env.begin() as txn:
        entries_db = env.open_db(Model._meta["entries_db_name"].encode("utf-8"),
                                txn=txn)
        with txn.cursor(entries_db) as cursor:
            raw = cursor.get(struct.pack("!Q", 0))

    retrieved = pickle.loads(raw)

    assert saved == retrieved


def test_create_returns_entry(tmpdir):

    doc = {'key1': 'value1', 'key2': 'value2'}

    with Model.open(tmpdir) as db:
        entry = db.create(**doc)

        assert entry == doc, entry
        assert entry.pk == 0
        assert entry.saved


def test_create_is_incremental(tmpdir):

    with Model.open(tmpdir) as db:
        entry0 = db.create()
        entry1 = db.create()

        assert entry0.pk == 0
        assert entry1.pk == 1


def test_bulk_create(tmpdir):

    with Model.open(tmpdir) as db:
        entries = [Model(data=i) for i in range(10)]

        db.bulk_create(entries)

        for entry in entries:
            assert entry.saved
            assert entry.pk == entry['data']


def test_bulk_create_multiple_times(tmpdir):

    with Model.open(tmpdir) as db:
        entries1 = [Model(data=i) for i in range(10)]
        db.bulk_create(entries1)

        entries2 = [Model(data=i) for i in range(10, 20)]
        db.bulk_create(entries2)

        for entry in entries2:
            assert entry.saved
            assert entry.pk == entry['data']


def test_create_index_collision(tmpdir):
    from binlog.exceptions import IntegrityError

    env = lmdb.open(str(tmpdir), max_dbs=1)
    with env.begin(write=True) as txn:
        entries_db = env.open_db(Model._meta["entries_db_name"].encode("utf-8"),
                                txn=txn)
        with txn.cursor(entries_db) as cursor:
            raw = cursor.put(struct.pack("!Q", 0),
                             b'I am not supposed to be here!')

    with Model.open(tmpdir) as db:
        with pytest.raises(IntegrityError):
            db.create(data='test')


def test_bulk_create_index_collision(tmpdir):
    from binlog.exceptions import IntegrityError

    env = lmdb.open(str(tmpdir), max_dbs=1)
    with env.begin(write=True) as txn:
        entries_db = env.open_db(Model._meta["entries_db_name"].encode("utf-8"),
                                txn=txn)
        with txn.cursor(entries_db) as cursor:
            raw = cursor.put(struct.pack("!Q", 5),
                             b'I am not supposed to be here!')

    with Model.open(tmpdir) as db:
        entries = [Model(data=i) for i in range(10)]
        with pytest.raises(IntegrityError):
            db.bulk_create(entries)
