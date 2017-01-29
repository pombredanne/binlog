import math
import operator as op

from binlog.abstract import IterSeek, Direction


class BinaryIterSeek(IterSeek):
    def __init__(self, a, b):
        self.a = a
        self.b = b

        if self.a.direction != self.b.direction:
            raise ValueError("both iterseeks must have the same direction")

        self.direction = self.a.direction


class ANDIterSeek(BinaryIterSeek):
    def seek(self, value):
        self.a.seek(value)
        self.b.seek(value)

    def __next__(self):
        a_val = next(self.a)
        b_val = next(self.b)
        while True:
            if a_val > b_val:
                if self.direction is Direction.F:
                    self.b.seek(a_val)
                    b_val = next(self.b)
                else:
                    self.a.seek(b_val)
                    a_val = next(self.a)
            elif a_val < b_val:
                if self.direction is Direction.F:
                    self.a.seek(b_val)
                    a_val = next(self.a)
                else:
                    self.b.seek(a_val)
                    b_val = next(self.b)
            else:
                return a_val


class ORIterSeek(BinaryIterSeek):
    def __init__(self, a, b):
        super().__init__(a, b)
        self._iter = self._nextiter()

    def _nextiter(self):
        if self.direction is Direction.F:
            comp = min
            limit = math.inf
        else:
            comp = max
            limit = -math.inf

        a = (self.a, next(self.a, limit))
        b = (self.b, next(self.b, limit))
        v = None
    
        while v is not limit:
            c, v = comp((a, b),
                        key=op.itemgetter(1),
                        default=(None, limit))
    
            if v is not limit:
                yield v
                if a[1] == b[1]:
                    a = (self.a, next(self.a, limit))
                    b = (self.b, next(self.b, limit))
                elif c is self.a:
                    a = (self.a, next(self.a, limit))
                else:
                    b = (self.b, next(self.b, limit))

    def seek(self, value):
        self.a.seek(value)
        self.b.seek(value)
        self._iter = self._nextiter()

    def __next__(self):
        return next(self._iter)
