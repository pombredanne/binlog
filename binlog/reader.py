import os
import pickle

from acidfile import ACIDFile
from bsddb3 import db

from .binlog import Binlog, Record
from .constants import LOGINDEX_NAME, CHECKPOINT_DIR
from .cursor import Cursor
from .register import Register


class Reader(Binlog):
    def __init__(self, path, checkpoint=None):
        self.env = self.open_environ(path, create=False)

        self.logindex = self.open_logindex(self.env, LOGINDEX_NAME)
        self.li_cursor = Cursor(self.logindex)
        self.li_cursor.first()
        self.last_liidx = None 

        self.register = None 

        self.retry = False

        self.current_log = None
        self.cl_cursor = None

        if checkpoint is None:
            self.checkpoint = None
        else:
            self.checkpoint = os.path.join(path, CHECKPOINT_DIR, checkpoint)
            self.load()

        if self.register is None:
            self.register = Register()


    def next(self, next_log=False):
        if not self.retry:
            pos = self.register.next(log=next_log)
            try:
                self.set_cursors(pos)
            except db.DBInvalidArgError as exc:
                errcode, _ = exc.args
                if errcode == 22:
                    self.retry = True
                    return None
                else:  # pragma: no cover
                    raise
            except db.DBNoSuchFileError as exc:
                self.retry = True
                return None
        else:
            self.retry = False

        if self.cl_cursor is None:
            try:
                self.set_cursors(self.register.current)
            except db.DBInvalidArgError as exc:
                errcode, _ = exc.args
                self.retry = True
                if errcode == 22:
                    return None
                else:  # pragma: no cover
                    raise

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

    def load(self):
        if self.checkpoint is None:
            raise ValueError('checkpoint was not set')

        try:
            with ACIDFile(self.checkpoint, mode='rb') as cp:
                self.register = pickle.load(cp)
        except:
            return False
        else:
            self.register.reset()
            return True

    def save(self):
        if self.checkpoint is None:
            raise ValueError('checkpoint was not set')

        if not os.path.isdir(self.checkpoint):
            os.makedirs(self.checkpoint)

        try:
            with ACIDFile(self.checkpoint, mode='wb') as cp:
                pickle.dump(self.register, cp)
        except:  # pragma: no cover
            return False
        else:
            return True

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
        if self.cl_cursor is None or rec.liidx != self.last_liidx:
            self.last_liidx = rec.liidx
            self.li_cursor.idx = rec.liidx
            _, logname = self.li_cursor.current()
            self.current_log = db.DB(self.env)
            self.current_log.open(logname.decode('utf-8'),
                                  None, db.DB_RECNO, db.DB_RDONLY)

            self.cl_cursor = Cursor(self.current_log, rec.clidx)
        else:
            self.cl_cursor.idx = rec.clidx

    def status(self):
        res = {}

        li_idx = self.li_cursor.idx

        data = self.li_cursor.first()
        while data is not None:
            idx, name = data
            try:
                cdb = db.DB(self.env)
                cdb.open(name.decode('utf-8'), None,
                         db.DB_RECNO, db.DB_RDONLY)
            except db.DBNoSuchFileError:
                pass
            else:
                cur = cdb.cursor()
                cdata = cur.last()
                cur.close()
                cdb.close()
                if cdata is not None:
                    cidx, _ = cdata
                    reg = self.register.reg.get(idx)
                    res[idx] = [(1, cidx)] == reg
                    if not reg and idx > 1:
                        res[idx - 1] = False

            data = self.li_cursor.next()

        self.li_cursor.idx = li_idx
        return res
