import os
import pickle

from acidfile import ACIDFile
from bsddb3 import db

from .binlog import Binlog, Record
from .constants import LOGINDEX_NAME
from .cursor import Cursor
from .register import Register


class Reader(Binlog):
    def __init__(self, path, checkpoint=None):
        self.env = self.open_environ(path)
        self.logindex = self.open_logindex(self.env, LOGINDEX_NAME)
        self.register = Register()
        self.retry = False

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
                    self.register = pickle.load(cp)
            except:
                self.li_cursor = Cursor(self.logindex)

                self.current_log = None
                self.cl_cursor = None
            else:
                self.register.reset()
                self.li_cursor = Cursor(self.logindex)

    def next(self, next_log=False):
        if not self.retry:
            pos = self.register.next(log=next_log)
            self.set_cursors(pos)
        else:
            self.retry = False

        try:
            data = self.cl_cursor.current()
        except db.DBInvalidArgError as exc:
            errcode, _ = exc.args
            if errcode == 22:
                data = None
            else:  # pragma: no cover
                raise

        if data is None:
            if self.has_next_log():
                return self.next(next_log=True)
            else:
                self.retry = True
                return None
        else:
            _, value = data
            return pickle.loads(value)


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
            pickle.dump(self.register, cp)

    def ack(self, record):
        """Acknowledge some data given by `next_record`."""
        self.register.add(record)

    def has_next_log(self):
        """Returns `True` if there is a next event log."""
        last_idx = self.li_cursor.idx
        try:
            idx, _ = self.li_cursor.next()
        except:
            return False
        else:
            return True
        finally:
            self.li_cursor.idx = last_idx

    def set_cursors(self, rec):
        self.li_cursor.idx = rec.liidx
        _, logname = self.li_cursor.current()
        self.current_log = db.DB(self.env)
        self.current_log.open(logname.decode('utf-8'),
                              None, db.DB_RECNO, db.DB_RDONLY)
        self.cl_cursor = Cursor(self.current_log, rec.clidx)
