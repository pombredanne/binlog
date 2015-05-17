import os
import pickle

from acidfile import ACIDFile
from bsddb3 import db

from .binlog import Binlog, Record
from .constants import LOGINDEX_NAME
from .cursor import Cursor


class Reader(Binlog):
    def __init__(self, path, checkpoint=None):
        self.env = self.open_environ(path)
        self.logindex = self.open_logindex(self.env, LOGINDEX_NAME)

        if checkpoint is not None:
            self.checkpoint = os.path.join(path, checkpoint)
        else:
            self.checkpoint = None
            self.li_cursor = Cursor(self.logindex)

            self.current_log = None
            self.cl_cursor = None

        if checkpoint is not None:
            try:
                with ACIDFile(self.checkpoint, mode='rb') as cp:
                    data = pickle.load(cp)
                liidx = data['liidx']
                clidx = data['clidx']
            except:
                self.li_cursor = Cursor(self.logindex)

                self.current_log = None
                self.cl_cursor = None
            else:
                self.li_cursor = Cursor(self.logindex, liidx)
                data = self.li_cursor.current()
                if data is not None:
                    _, value = data
                    self.current_log = db.DB(self.env)
                    self.current_log.open(value.decode('utf-8'),
                                          None, db.DB_RECNO, db.DB_RDONLY)
                    self.cl_cursor = Cursor(self.current_log, clidx)

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

    def next_record(self):
        data = self.next()
        if data is None:
            return None
        else:
            return Record(liidx=self.li_cursor.idx,
                          clidx=self.cl_cursor.idx,
                          value=data)

    def save(self):
        if self.checkpoint is None:
            raise ValueError('checkpoint was not set')

        with ACIDFile(self.checkpoint, mode='wb') as cp:
            try:
                clidx = self.cl_cursor.idx
            except:
                clidx = None

            try:
                liidx = self.li_cursor.idx
            except:
                liidx = None

            pickle.dump({'clidx': clidx, 'liidx': liidx}, cp)
