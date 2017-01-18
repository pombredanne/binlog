from functools import partial
from operator import attrgetter
import abc
import struct
import pickle


class Serializer(metaclass=abc.ABCMeta):
    @staticmethod
    @abc.abstractmethod
    def python_value(self, value):  # pragma: no cover
        pass

    @staticmethod
    @abc.abstractmethod
    def db_value(self, value):  # pragma: no cover
        pass


class NumericSerializer(Serializer):
    @staticmethod
    def python_value(value):
        return struct.unpack("@Q", value)[0]

    db_value = staticmethod(partial(struct.pack, "@Q"))


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
