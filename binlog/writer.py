import os
import pickle

from bsddb3 import db

from .binlog import Binlog
from .constants import *


class Writer(Binlog):
    def __init__(self, path, max_log_events=MAX_LOG_EVENTS):
        self.env = self.open_environ(path)
        self.logindex = self.open_logindex(self.env, LOGINDEX_NAME)
        self.max_log_events = max_log_events

    @property
    def current_log(self):
        """Return the log DB for the current write."""
        cursor = self.logindex.cursor()
        last = cursor.last()
        cursor.close()

        if not last:
            name = LOG_PREFIX + '.1'
            self.logindex.append(name)
            self.logindex.sync()
        else:
            idx, value = last
            name = value.decode('utf-8')

        log = db.DB(self.env)
        log.open(name, None, db.DB_RECNO, db.DB_CREATE)

        cursor = log.cursor()
        last = cursor.last()
        cursor.close()

        if last:
            eidx, _ = last
            if eidx >= self.max_log_events:
                name = LOG_PREFIX + '.' + str(idx+1)

                self.logindex.append(name)
                self.logindex.sync()
                log.close()

                log = db.DB(self.env)
                log.open(name, None, db.DB_RECNO, db.DB_CREATE)

        return log

    def append(self, data):
        """Append data to the current log DB."""
        self.current_log.append(pickle.dumps(data))
