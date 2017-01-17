import re
from .index import Index


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
    pass
