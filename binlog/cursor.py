class CursorMethod:
    def __init__(self, db, name, cursor):
        self.db = db
        self.name = name
        self.cursor = cursor

    def __call__(self, *args, **kwargs):
        try:
            cursor = self.db.cursor()
            if self.cursor.idx is not None:
                cursor.set(self.cursor.idx)
            data = getattr(cursor, self.name)(*args, **kwargs)
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
