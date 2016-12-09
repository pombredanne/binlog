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
