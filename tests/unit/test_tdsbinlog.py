import pytest
from bsddb3 import db


def test_TDSBinlog_exists():
    from binlog import binlog

    assert hasattr(binlog, 'TDSBinlog')


def test_TDSBinlog_flags_are_correct():
    from binlog.binlog import TDSBinlog

    assert db.DB_CREATE & TDSBinlog.flags
    assert db.DB_INIT_MPOOL & TDSBinlog.flags
    assert db.DB_INIT_LOCK & TDSBinlog.flags
    assert db.DB_INIT_TXN & TDSBinlog.flags
