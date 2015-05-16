import os

from bsddb3 import db


class Writer:
    @staticmethod
    def create_environ(path):
        if not os.path.isdir(path):
            if os.path.exists(path):
                raise ValueError('%s is not a directory' % path)
            else:
                os.makedirs(path)

        env = db.DBEnv()
        env.open(path, db.DB_INIT_CDB|db.DB_INIT_MPOOL|db.DB_CREATE)
        return env
