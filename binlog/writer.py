import os
import pickle

from bsddb3 import db


LOGINDEX_NAME = 'logindex'
LOG_PREFIX = 'events'
MAX_LOG_EVENTS = 100000


class Writer:
    def __init__(self, path, max_log_events=MAX_LOG_EVENTS):
        self.env = self.open_environ(path)
        self.logindex = self.open_logindex(self.env, LOGINDEX_NAME)
        self.max_log_events = max_log_events

    @staticmethod
    def open_environ(path):
        """Open or create the db environment."""
        if not os.path.isdir(path):
            if os.path.exists(path):
                raise ValueError('%s is not a directory' % path)
            else:
                os.makedirs(path)

        env = db.DBEnv()
        env.open(path, db.DB_INIT_CDB|db.DB_INIT_MPOOL|db.DB_CREATE)
        return env

    @staticmethod
    def open_logindex(env, filename):
        """Open or create the logindex inside the environment."""
        logindex = db.DB(env)
        try:
            logindex.open(filename, None, db.DB_RECNO, db.DB_CREATE)
        except db.DBError as exc:
            errcode, name = exc.args
            if errcode == 21:  # Is a directory
                raise ValueError('%s is a directory' % filename) from exc
            else:
                raise

        return logindex

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
