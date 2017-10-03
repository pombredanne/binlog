import re

from .connection import Connection
from .exceptions import BadUsageError
from .index import Index
from .serializer import NumericSerializer, ObjectSerializer


class ModelMeta(type):
    def __new__(cls, name, bases, namespace, **kwds):
        _indexes = dict()
        _meta = {
            'config_db_name': 'Config',
            'entries_db_name': 'Entries',
            'checkpoints_db_name': 'Checkpoints',
            'index_db_format': ('{model._meta[entries_db_name]}'
                                '__idx__'
                                '{index_name}'),
            'readers_env_directory': 'readers',
            'data_env_directory': 'data',
            'connection_class': Connection}
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
    K = NumericSerializer
    V = ObjectSerializer

    def __init__(self, *args, **kwargs):
        self.pk = None
        self.saved = False

        super().__init__(*args, **kwargs)

    @classmethod
    def open(cls, path, **kwargs):
        # This MUST be imported every time because can be invalidated by
        # `reset_connections`.
        from .connectionmanager import PROCESS_CONNECTIONS

        return PROCESS_CONNECTIONS.open(
            cls,
            path,
            cls._meta['connection_class'],
            kwargs)

    def mark_as_saved(self, pk):
        self.pk = pk
        self.saved = True

    def save(self, pk, db, txn):
        with txn.cursor(db) as cursor:
            success = cursor.put(self.K.db_value(pk),
                                 self.V.db_value(self.copy()),
                                 overwrite=False)

            if success:
                self.mark_as_saved(pk)

            return success

    @classmethod
    def reindex(cls, path, **kwargs):
        with cls.open(path, **kwargs) as conn:
            print("Drop")
            conn._drop_indexes()

        with cls.open(path, **kwargs) as conn:
            print("Create")
            with conn.data(write=True):
                pass

        with cls.open(path, **kwargs) as conn:
            print("Reindex")
            conn._reindex()
