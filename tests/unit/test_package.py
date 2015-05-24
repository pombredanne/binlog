import pytest
from importlib import import_module


def can_import(package_name):
    try:
        import_module(package_name)
    except ImportError:
        return False
    else:
        return True


def test_import_binlog():
    assert can_import('binlog')


def test_import_binlog_writer():
    assert can_import('binlog.writer')


def test_import_binlog_binlog():
    assert can_import('binlog.binlog')


def test_import_binlog_constants():
    assert can_import('binlog.constants')


def test_import_binlog_reader():
    assert can_import('binlog.reader')


def test_import_binlog_register():
    assert can_import('binlog.register')


def test_import_binlog_queue():
    assert can_import('binlog.queue')
