import os
import pickle

from bsddb3 import db

from .binlog import Binlog
from .constants import *


class Writer(Binlog):
    def __init__(self, path, max_log_events=MAX_LOG_EVENTS):
        self.path = path
        self.env = self.open_environ(path)
        self.logindex = self.open_logindex(self.env, LOGINDEX_NAME)
        self.max_log_events = max_log_events
        self._current_log = None
        self.next_will_create_log = False
        self._current_idx = None

    def set_current_log(self):
        """Return the log DB for the current write."""
        cursor = self.logindex.cursor()
        last = cursor.last()
        cursor.close()

        if not last:
            name = LOG_PREFIX + '.1'
            self.logindex.append(name)
            self.logindex.sync()
            self._current_idx = 1
            log = db.DB(self.env)
            log.open(name, None, db.DB_RECNO, db.DB_CREATE)
        else:
            idx, value = last
            self._current_idx = idx
            name = value.decode('utf-8')

            log = db.DB(self.env)
            try:
                log.open(name, None, db.DB_RECNO)
            except Exception as exc:
                # The last db was deleted
                name = LOG_PREFIX + '.' + str(idx + 1)
                self._current_idx = idx + 1

                log = db.DB(self.env)
                log.open(name, None, db.DB_RECNO, db.DB_CREATE)


        cursor = log.cursor()
        last = cursor.last()
        cursor.close()

        if last:
            eidx, _ = last
            if eidx >= self.max_log_events:
                log.close()

                name = LOG_PREFIX + '.' + str(idx+1)
                self.logindex.append(name)
                self.logindex.sync()
                self._current_idx = idx + 1

                log = db.DB(self.env)
                log.open(name, None, db.DB_RECNO, db.DB_CREATE)

        self._current_log = log
        return self._current_log

    def append(self, data):
        """Append data to the current log DB."""
        if self.next_will_create_log:
            self.next_will_create_log = False
            if self._current_log is not None:  # pragma: no branch
                self._current_log.close()
                self._current_log = None
            else:  # pragma: no cover
                pass

        if self._current_log is None:
            self.set_current_log()

        idx = self._current_log.append(pickle.dumps(data))

        self.next_will_create_log = (idx >= self.max_log_events)

    def delete(self, idx):
        """Delete a log DB."""
        cursor = self.logindex.cursor()
        try:
            data = cursor.last()
            if data is None:
                raise ValueError('Invalid database')
            else:
                last, _ = data
                if last == idx:
                    raise ValueError('Cannot delete the current database.')
        except:
            raise
        else:
            txn = self.env.txn_begin(flags=db.DB_TXN_NOWAIT)
            dbname = os.path.join(self.path, LOG_PREFIX + '.' + str(idx))
            try:
                self.env.dbremove(dbname, txn=txn)
                self.logindex.delete(idx)
            except Exception as exc:
                txn.abort()
                raise ValueError('Cannot delete this database') from exc
            else:
                txn.commit()
        finally:
            cursor.close()

