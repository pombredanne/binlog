from binlog.abstract import IterSeek, Direction
from binlog.registry import S


class BinaryIterSeek(IterSeek):
    def __init__(self, *things):
        self.things = list()

        for thing in things:
            if isinstance(thing, self.__class__):
                self.things.extend(thing.things)
            else:
                self.things.append(thing)

        if len(set(t.direction for t in self.things)) != 1:
            raise ValueError("both iterseeks must have the same direction")

        self.direction = self.things[0].direction
        self.seeked = None

    def seek(self, value):
        self.seeked = value


class ANDIterSeek(BinaryIterSeek):
    def __next__(self):
        first, rest = self.things[0], self.things[1:]
        if self.seeked is not None and S.MIN <= self.seeked <= S.MAX:
            first.seek(self.seeked)
            self.seeked = None
        for cf in first:
            for t in rest:
                t.seek(cf)
                ct = next(t)
                if cf != ct:
                    first.seek(ct)
                    break
            else:
                self.seeked = cf + self.direction.value
                return cf
        else:
            raise StopIteration


class ORIterSeek(BinaryIterSeek):
    def __next__(self):
        if self.direction is Direction.F:
            comp = min
            R = float('inf')
        else:
            comp = max
            R = float('-inf')

        for t in self.things:
            if self.seeked is not None:
                t.seek(self.seeked)
            c = next(t, R)
            if c == self.seeked:
                self.seeked += self.direction.value
                return c
            else:
                R = comp((R, c))
        if R < S.MIN or R > S.MAX:
            raise StopIteration
        else:
            self.seeked = R + self.direction.value
            return R
