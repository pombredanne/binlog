import pytest


def test_serializer_can_be_imported():
    try:
        from binlog.serializer import Serializer
    except ImportError as exc:
        assert False, exc


def test_serializer_is_abstract():
    from binlog.serializer import Serializer

    with pytest.raises(TypeError):
        Serializer()

    class MySerializer(Serializer):
        def python_value(self, value):
            pass

        def db_value(self, value):
            pass

    # SHOULD NOT RAISE
    s = MySerializer()
