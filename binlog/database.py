import abc
from collections import namedtuple
from contextlib import contextmanager

from .serializer import NumericSerializer
from .serializer import ObjectSerializer
from .serializer import TextSerializer


class CursorProxy(namedtuple('_CursorProxy', ['db', 'cursor'])):
    def get(self, key, default=None):
        raw = self.cursor.get(self.db.K.db_value(key), default=None)
        if raw is None:
            return default
        else:
            return self.db.V.python_value(raw)

    def put(self, key, value, **kwargs):
        return self.cursor.put(self.db.K.db_value(key),
                               self.db.V.db_value(value),
                               **kwargs)

    def putmulti(self, items, **kwargs):
        def _translate_items():
            for key, value in items:
                yield (self.db.K.db_value(key), self.db.V.db_value(value))

        return self.cursor.putmulti(_translate_items(), **kwargs)


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


class Config(Database):
    K = TextSerializer
    V = ObjectSerializer


class Entries(Database):
    K = NumericSerializer
    V = ObjectSerializer


class Checkpoints(Database):
    K = TextSerializer
    V = ObjectSerializer
