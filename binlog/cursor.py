import bsddb3
import time


class CursorMethod:
    def __init__(self, db, name, cursor):
        self.db = db
        self.name = name
        self.cursor = cursor

    def __call__(self, *args, **kwargs):
        try:
            while True:
                cursor = self.db.cursor()
                if self.cursor.idx is not None:
                    cursor.set(self.cursor.idx)
                try:
                    data = getattr(cursor, self.name)(*args, **kwargs)
                except bsddb3.db.DBLockDeadlockError:
                    print("Deadlock avoided!")
                    time.sleep(0.1)
                else:
                    break
                finally:
                    cursor.close()
        except:
            raise
        else:
            if data is not None:
                self.cursor.idx, _ = data
            return data
        finally:
            cursor.close()


class Cursor:
    def __init__(self, db, idx=None):
        self.db = db
        self.idx = idx

    def __getattr__(self, name):
        return CursorMethod(self.db, name, self)
