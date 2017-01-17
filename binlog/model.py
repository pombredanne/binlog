import re


class BinlogMeta(type):
    def __new__(cls, name, bases, namespace, **kwds):
        result = type.__new__(cls, name, bases, namespace)

        # Replace any __meta_*__ by an entry in the _meta dict.
        result._meta = {
            'metadb_name': 'Meta',
            'entriesdb_name': 'Entries',
            'indexdb_format': ('{model._meta[entries_db_name]}'
                               '__idx__'
                               '{index.name}')}
        for attr in namespace.copy():
            m = re.match('^__meta_(.*)__$', attr)
            if m:
                result._meta[m.group(1)] = namespace.pop(attr)

        return result


class Binlog(dict, metaclass=BinlogMeta):
    pass
