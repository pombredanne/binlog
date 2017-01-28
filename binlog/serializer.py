from datetime import datetime, timedelta
from functools import partial
import calendar
import pickle
import struct
import time

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
    db_value = staticmethod(partial(pickle.dumps,
                                    protocol=pickle.HIGHEST_PROTOCOL))


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


class DatetimeSerializer(Serializer):
    @staticmethod
    def python_value(value):
        int_val = NumericSerializer.python_value(value)
        timestamp = datetime(*time.gmtime(int_val // 1000000)[:6])
        return timestamp + timedelta(microseconds=int_val % 1000000)

    @staticmethod
    def db_value(value):
        timestamp = int(calendar.timegm(value.timetuple())) * 1000000
        int_val = timestamp + value.microsecond
        return NumericSerializer.db_value(int_val)
