import pytest
from bsddb3 import db


def test_CDSBinlog_exists():
    from binlog import binlog

    assert hasattr(binlog, 'CDSBinlog')


def test_CDSBinlog_flags_are_correct():
    from binlog.binlog import CDSBinlog

    assert db.DB_CREATE & CDSBinlog.flags
    assert db.DB_INIT_MPOOL & CDSBinlog.flags
    assert db.DB_INIT_CDB & CDSBinlog.flags
