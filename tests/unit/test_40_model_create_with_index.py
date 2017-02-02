import lmdb
import os
import pytest

from binlog.index import TextIndex
from binlog.model import Model
from binlog.serializer import NumericSerializer

class IndexedModel(Model):
    name = TextIndex(mandatory=True)
    address = TextIndex(mandatory=False)


def test_model_create_use_index(tmpdir):

    with IndexedModel.open(tmpdir) as db:
        db.create(name='name_value', address='address_value')
    
    path = os.path.join(str(tmpdir), Model._meta["data_env_directory"])
    env = lmdb.open(path, max_dbs=3)
    with env.begin() as txn:
        index_template = Model._meta["index_db_format"]
        for field, value in zip(('name', 'address'),
                                (b'name_value', b'address_value')):
            index_db = env.open_db(
                index_template.format(model=IndexedModel,
                                      index_name=field).encode('utf-8'),
                txn=txn)
            with txn.cursor(index_db) as cursor:
                raw = cursor.get(value)
                assert raw == NumericSerializer.db_value(0)


def test_mandatory_index_not_provided__create(tmpdir):
    with IndexedModel.open(tmpdir) as db:
        with pytest.raises(ValueError):
            db.create(address='address_value')


def test_mandatory_index_not_provided_abort_transaction__create(tmpdir):
    with IndexedModel.open(tmpdir) as db:
        try:
            db.create(address='address_value')
            assert False, "DID NOT RAISE"
        except ValueError:
            assert not list(db.reader())


def test_mandatory_index_not_provided__bulk_create(tmpdir):
    with IndexedModel.open(tmpdir) as db:
        with pytest.raises(ValueError):
            db.bulk_create([IndexedModel(address='address_value')])


def test_mandatory_index_not_provided_abort_transaction__bulk_create(tmpdir):
    with IndexedModel.open(tmpdir) as db:
        try:
            db.bulk_create([IndexedModel(address='address_value')])
            assert False, "DID NOT RAISE"
        except ValueError:
            assert not list(db.reader())


def test_non_mandatory_index_not_provided(tmpdir):
    with IndexedModel.open(tmpdir) as db:
        db.create(name='name_value')

    path = os.path.join(str(tmpdir), Model._meta["data_env_directory"])
    env = lmdb.open(path, max_dbs=3)
    with env.begin() as txn:
        index_template = Model._meta["index_db_format"]
        index_db = env.open_db(
            index_template.format(model=IndexedModel,
                                  index_name='address').encode('utf-8'),
            txn=txn)
        with txn.cursor(index_db) as cursor:
            assert not list(cursor)
