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
        return value.tobytes().decode('utf-8')

    @staticmethod
    def db_value(value):
        return value.encode('utf-8')


class ObjectSerializer(Serializer):
    python_value = staticmethod(pickle.loads)
    db_value = staticmethod(pickle.dumps)


class NullListSerializer(Serializer):
    @staticmethod
    def python_value(value):
        return value.tobytes().replace(b"\0", b".").decode('ascii')

    @staticmethod
    def db_value(value):
        try:
            assert value
            assert '\0' not in value
            return value.encode('ascii').replace(b'.', b'\0')
        except (AssertionError, UnicodeEncodeError) as exc:
            raise ValueError from exc
