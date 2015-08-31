from collections import namedtuple
import os

from bsddb3 import db

Record = namedtuple('Record', ['liidx', 'clidx', 'value'])


class Binlog:

    flags = None

    @classmethod
    def open_environ(cls, path, create=True):
        """Open or create the db environment."""
        if not os.path.isdir(path):
            if os.path.exists(path):
                raise ValueError('%s is not a directory' % path)
            else:
                if create:
                    os.makedirs(path)
                else:
                    raise ValueError('environment does not exists.')

        env = db.DBEnv()

        if cls.flags is None:
            raise NotImplementedError("`flags` attribute must be setted.")

        env.open(path, cls.flags)

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


class TDSBinlog(Binlog):
    """
    Binlog with Transactional Data Store backend.

    This backend support only one concurrent writer and multiple readers.

    THIS IS UNSAFE WITH MULTIPLE READERS!

    """
    flags = db.DB_CREATE | db.DB_INIT_MPOOL | db.DB_INIT_LOCK | db.DB_INIT_TXN


class CDSBinlog(Binlog):
    """
    Binlog with Concurrent Data Store backend.

    You can use this backend with multiple writers, but only one can
    write at a time. The locking is managed by the database.

    """
    flags = db.DB_CREATE | db.DB_INIT_MPOOL | db.DB_INIT_CDB
