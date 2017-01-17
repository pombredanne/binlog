import pytest


@pytest.mark.wip
def test_binlog_exists():
    try:
        from binlog.model import Binlog
    except ImportError as exc:
        assert False, exc


@pytest.mark.wip
def test_binlog_have_meta_with_defaults():
    from binlog.model import Binlog

    b = Binlog()
    assert b._meta['metadb_name'] == 'Meta'
    assert b._meta['entriesdb_name'] == 'Entries'
    assert b._meta['indexdb_format'] == ('{model._meta[entries_db_name]}'
                                          '__idx__'
                                          '{index.name}')


@pytest.mark.wip
def test_binlog_subclass_can_override_meta_values():
    from binlog.model import Binlog

    OVERRIDED = 'OVERRIDED'

    class CustomBinlog(Binlog):
        __meta_metadb_name__ = OVERRIDED 

    b = CustomBinlog()
    assert b._meta['metadb_name'] is OVERRIDED


@pytest.mark.wip
def test_binlog_inherits_from_dict():
    from binlog.model import Binlog

    assert issubclass(Binlog, dict)

