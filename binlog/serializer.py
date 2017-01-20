from functools import partial
import struct
import pickle

from .abstract import Serializer


class NumericSerializer(Serializer):
    @staticmethod
    def python_value(value):
        return struct.unpack("!Q", value)[0]

    db_value = staticmethod(partial(struct.pack, "!Q"))


class TextSerializer(Serializer):
    @staticmethod
    def python_value(value):
        return value.decode('utf-8')

    @staticmethod
    def db_value(value):
        return value.encode('utf-8')


class ObjectSerializer(Serializer):
    python_value = staticmethod(pickle.loads)
    db_value = staticmethod(pickle.dumps)


class StringListSerializer(Serializer):
    @staticmethod
    def python_value(value):
        return [x.decode('utf-8') for x in value.split(b'\0')]

    @staticmethod
    def db_value(value):
        try:
            assert value
            assert all(bool(v) for v in value)
            assert not any('\0' in v for v in value)
            return b'\0'.join(x.encode('ascii') for x in value)
        except (AssertionError, UnicodeEncodeError) as exc:
            raise ValueError from exc
