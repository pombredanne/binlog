from contextlib import contextmanager
import abc

from .cursor import CursorProxy


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
    @contextmanager
    def cursor(cls, res):
        db_handler = res.db.get(cls.__name__.lower())
        with res.txn.cursor(db_handler) as cursor:
            yield CursorProxy(cls, cursor)


class Serializer(metaclass=abc.ABCMeta):
    @staticmethod
    @abc.abstractmethod
    def python_value(self, value):  # pragma: no cover
        pass

    @staticmethod
    @abc.abstractmethod
    def db_value(self, value):  # pragma: no cover
        pass
