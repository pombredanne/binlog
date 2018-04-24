from collections.abc import Iterator
from contextlib import contextmanager
import abc
import enum


class Direction(enum.Enum):
    F = 1   #: Forward
    B = -1  #: Backward


class Database(metaclass=abc.ABCMeta):
    @abc.abstractproperty
    def K(self):  # pragma: no cover
        """ Key serializer """
        pass

    @abc.abstractproperty
    def V(self):  # pragma: no cover
        """ Value serializer """
        pass

    @classmethod
    def get_db_handler(cls, res, db_name=None):
        db_name = cls.__name__.lower() if db_name is None else db_name
        return res.db.get(db_name)

    @classmethod
    @contextmanager
    def cursor(cls, res, db_name=None, direction=Direction.F):
        from .cursor import CursorProxy

        db_name = cls.__name__.lower() if db_name is None else db_name
        db_handler = res.db.get(db_name)
        with res.txn.cursor(db_handler) as cursor:
            yield CursorProxy(cls, res, cursor, db_name, direction=direction)


class Serializer(metaclass=abc.ABCMeta):
    @staticmethod
    @abc.abstractmethod
    def python_value(self, value):  # pragma: no cover
        pass

    @staticmethod
    @abc.abstractmethod
    def db_value(self, value):  # pragma: no cover
        pass


class IterSeek(Iterator):
    @abc.abstractmethod
    def seek(self, pos):  # pragma: no cover
        pass

    def __and__(self, other):
        from .operations import ANDIterSeek
        return ANDIterSeek(self, other)

    def __or__(self, other):
        from .operations import ORIterSeek
        return ORIterSeek(self, other)
