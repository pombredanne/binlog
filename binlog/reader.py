import pickle

from bsddb3 import db

from .binlog import Binlog
from .constants import LOGINDEX_NAME


class Reader(Binlog):
    def __init__(self, path):
        self.env = self.open_environ(path)
        self.logindex = self.open_logindex(self.env, LOGINDEX_NAME)
        self._current_log = None
        self._logindex_idx = None
        self._current_log_idx = None

    @property
    def current_log(self):
        if self._current_log is None:

            cursor = self.logindex.cursor()
            if self._logindex_idx is None:
                data = cursor.first()
            else:
                cursor.set(self._logindex_idx)
                data = cursor.next()
            cursor.close()

            if data is not None:
                self._logindex_idx, value = data
                log = db.DB(self.env)
                log.open(value.decode('utf-8'), None, db.DB_RECNO, db.DB_RDONLY)
                self._current_log = log
                return self._current_log
            else:
                return None
        else:
            return self._current_log

    def set_next_log(self):
        if self.current_log is None:
            return None
        else:
            self.current_log.close()
            self._current_log = None

        cursor = self.logindex.cursor()
        cursor.set(self._logindex_idx)
        data = cursor.next()
        cursor.close()

        if data is not None:
            self._current_log_idx, value = data
            log = db.DB(self.env)
            log.open(value.decode('utf-8'), None, db.DB_RECNO, db.DB_RDONLY)
            self._current_log = log
            self._current_log_idx = None
            return self._current_log
        else:
            return None

    def next(self, try_next=True):
        if self.current_log is not None:

            cursor = self.current_log.cursor()
            if self._current_log_idx is None:
                data = cursor.first()
            else:
                cursor.set(self._current_log_idx)
                data = cursor.next()
            cursor.close()

            if data is None:
                self.set_next_log()
                return self.next(try_next=False)
            else:
                self._current_log_idx, value = data
                return pickle.loads(value)
        else:
            return None
