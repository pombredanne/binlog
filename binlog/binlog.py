import os

from bsddb3 import db


class Binlog:
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
            else:  # pragma: no cover
                raise

        return logindex
