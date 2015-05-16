import pickle

from bsddb3 import db

from .binlog import Binlog
from .constants import LOGINDEX_NAME
from .cursor import Cursor


class Reader(Binlog):
    def __init__(self, path):
        self.env = self.open_environ(path)
        self.logindex = self.open_logindex(self.env, LOGINDEX_NAME)
        self.li_cursor = Cursor(self.logindex)

        self.current_log = None
        self.cl_cursor = None

    def set_current_log(self):
        if self.current_log is None:
            data = self.li_cursor.first()
            if data is not None:
                _, value = data
                self.current_log = db.DB(self.env)
                self.current_log.open(value.decode('utf-8'),
                                      None, db.DB_RECNO, db.DB_RDONLY)
                self.cl_cursor = Cursor(self.current_log)

    def set_next_log(self):
        data = self.li_cursor.next()
        if data is not None:
            _, value = data

            if self.current_log is not None:
                self.current_log.close()

            self.current_log = db.DB(self.env)
            self.current_log.open(value.decode('utf-8'),
                                  None, db.DB_RECNO, db.DB_RDONLY)
            self.cl_cursor = Cursor(self.current_log)
            return True
        else:
            return None

    def next(self):
        if self.current_log is None:
            self.set_current_log()

        if self.current_log is None:
            return None
        else:
            data = self.cl_cursor.next()
            if data is not None:
                _, value = data
                return pickle.loads(value)
            else:
                if self.set_next_log() is not None:
                    return self.next()
                else:
                    return None
