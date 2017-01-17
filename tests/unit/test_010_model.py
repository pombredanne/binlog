import pytest


def test_model_exists():
    try:
        from binlog.model import Model
    except ImportError as exc:
        assert False, exc


def test_model_have_meta_with_defaults():
    from binlog.model import Model

    b = Model()
    assert b._meta['metadb_name'] == 'Meta'
    assert b._meta['entriesdb_name'] == 'Entries'
    assert b._meta['indexdb_format'] == ('{model._meta[entries_db_name]}'
                                          '__idx__'
                                          '{index.name}')


def test_model_subclass_can_override_meta_values():
    from binlog.model import Model

    OVERRIDED = 'OVERRIDED'

    class CustomModel(Model):
        __meta_metadb_name__ = OVERRIDED 

    b = CustomModel()
    assert b._meta['metadb_name'] is OVERRIDED

    with pytest.raises(AttributeError):
        b.__meta_metadb_name__


def test_model_inherits_from_dict():
    from binlog.model import Model

    assert issubclass(Model, dict)
