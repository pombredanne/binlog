import pytest

from binlog.abstract import IterSeek


def test_cursorproxy_exists():
    try:
        from binlog.cursor import CursorProxy
    except ImportError as exc:
        assert False, exc


def test_cursorproxy_is_iterseek():
    from binlog.cursor import CursorProxy

    assert issubclass(CursorProxy, IterSeek)
