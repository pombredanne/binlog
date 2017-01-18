import pickle
import struct

import lmdb
import pytest

from binlog.model import Model


def test_create_store_entry(tmpdir):
    saved = {'key1': 'value1', 'key2': 'value2'}

    with Model.open(tmpdir) as db:
        success, entry = db.create(**saved)

    env = lmdb.open(str(tmpdir), max_dbs=1)
    with env.begin() as txn:
        entriesdb = env.open_db(Model._meta["entriesdb_name"].encode("utf-8"),
                                txn=txn)
        with txn.cursor(entriesdb) as cursor:
            raw = cursor.get(struct.pack("!Q", 0))

    retrieved = pickle.loads(raw)

    assert saved == retrieved


@pytest.mark.wip
def test_create_returns_entry(tmpdir):

    doc = {'key1': 'value1', 'key2': 'value2'}

    with Model.open(tmpdir) as db:
        success, entry = db.create(**doc)

        assert success
        assert entry == doc, entry
        assert entry.pk == 0
        assert entry.saved


def test_create_is_incremental(tmpdir):

    with Model.open(tmpdir) as db:
        success0, entry0 = db.create()
        success1, entry1 = db.create()

        assert success0 and success1
        assert entry0.pk == 0
        assert entry1.pk == 1
