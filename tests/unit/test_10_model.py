import struct

import lmdb
import pytest


def test_model_exists():
    try:
        from binlog.model import Model
    except ImportError as exc:
        assert False, exc


def test_model_have_meta_with_defaults():
    from binlog.model import Model

    b = Model()
    assert b._meta['config_db_name'] == 'Config'
    assert b._meta['entries_db_name'] == 'Entries'
    assert b._meta['checkpoints_db_name'] == 'Checkpoints'
    assert b._meta['index_db_format'] == ('{model._meta[entries_db_name]}'
                                          '__idx__'
                                          '{index_name}')
    assert b._meta['readers_env_directory'] == 'readers'
    assert b._meta['data_env_directory'] == 'data'


def test_model_subclass_can_override_meta_values():
    from binlog.model import Model

    OVERRIDED = 'OVERRIDED'

    class CustomModel(Model):
        __meta_config_db_name__ = OVERRIDED 

    b = CustomModel()
    assert b._meta['config_db_name'] is OVERRIDED

    with pytest.raises(AttributeError):
        b.__meta_config_db_name__


def test_model_inherits_from_dict():
    from binlog.model import Model

    assert issubclass(Model, dict)


def test_save_store_data(tmpdir):
    from binlog.model import Model

    env = lmdb.open(str(tmpdir), max_dbs=1)
    with env.begin(write=True) as txn:
        db = env.open_db(b'test', txn=txn)
        m = Model(data=0)
        assert m.save(0, db, txn)

    with env.begin(write=False) as txn:
        db = env.open_db(b'test', txn=txn)
        with txn.cursor(db) as cursor:
            assert cursor.get(struct.pack("!Q", 0))


def test_save_do_not_replace(tmpdir):
    from binlog.model import Model

    env = lmdb.open(str(tmpdir), max_dbs=1)

    with env.begin(write=True) as txn:
        db = env.open_db(b'test', txn=txn)
        m = Model(data=0)
        assert m.save(0, db, txn)

    with env.begin(write=False) as txn:
        db = env.open_db(b'test', txn=txn)
        with txn.cursor(db) as cursor:
            first = cursor.get(struct.pack("!Q", 0))

    with env.begin(write=True) as txn:
        db = env.open_db(b'test', txn=txn)
        m = Model(data=1)
        assert not m.save(0, db, txn)

    with env.begin(write=False) as txn:
        db = env.open_db(b'test', txn=txn)
        with txn.cursor(db) as cursor:
            second = cursor.get(struct.pack("!Q", 0))

    assert first == second
