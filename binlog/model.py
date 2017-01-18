import re

from .connection import Connection
from .index import Index
from .serializer import NumericSerializer, ObjectSerializer


class ModelMeta(type):
    def __new__(cls, name, bases, namespace, **kwds):
        _indexes = dict()
        _meta = {
            'metadb_name': 'Meta',
            'entriesdb_name': 'Entries',
            'indexdb_format': ('{model._meta[entries_db_name]}'
                               '__idx__'
                               '{index.name}')}
        for attr, value in namespace.copy().items():
            # Replace any __meta_*__ by an entry in the _meta dict.
            m = re.match('^__meta_(.*)__$', attr)
            if m:
                _meta[m.group(1)] = namespace.pop(attr)
                continue

            # Register Index subclasses in _index dict.
            if isinstance(value, Index):
                _indexes[attr] = namespace.pop(attr)

        result = type.__new__(cls, name, bases, namespace)
        result._indexes = _indexes
        result._meta = _meta

        return result


class Model(dict, metaclass=ModelMeta):
    def __init__(self, *args, **kwargs):
        self.pk = None
        self.saved = False
        super().__init__(*args, **kwargs)

    @classmethod
    def open(cls, path, **kwargs):
        return Connection(model=cls, path=path, kwargs=kwargs)

    def save(self, pk, db, txn):
        with txn.cursor(db) as cursor:
            success = cursor.put(NumericSerializer.db_value(pk),
                                 ObjectSerializer.db_value(self.copy()),
                                 overwrite=False)

            if success:
                self.pk = pk
                self.saved = True

            return success
