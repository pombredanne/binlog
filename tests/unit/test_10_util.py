import pytest


def test_MaskException_exists():
    try:
        from binlog.util import MaskException
    except ImportError as exc:
        assert False, exc


def test_MaskException_as_context_manager__masking():
    from binlog.util import MaskException

    with pytest.raises(TypeError):
        with MaskException(ValueError, TypeError):
            raise ValueError


def test_MaskException_as_context_manager__notmasking():
    from binlog.util import MaskException

    with pytest.raises(RuntimeError):
        with MaskException(ValueError, TypeError):
            raise RuntimeError


def test_MaskException_as_decorator__masking():
    from binlog.util import MaskException


    @MaskException(ValueError, TypeError)
    def test():
        raise ValueError

    with pytest.raises(TypeError):
        test()


def test_MaskException_as_decorator__notmasking():
    from binlog.util import MaskException

    @MaskException(ValueError, TypeError)
    def test():
        raise RuntimeError

    with pytest.raises(RuntimeError):
        test()
