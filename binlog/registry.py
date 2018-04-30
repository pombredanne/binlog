from bisect import insort, bisect_left
from collections import namedtuple
from itertools import count, cycle
import lmdb

from .abstract import IterSeek, Direction
from .exceptions import ReaderDoesNotExist
from .databases import Registry as RegistryDB
from .util import popminleft, consume
from .util import MaskException


MAXINT = 2**64-1


class S(namedtuple('Segment', ('L', 'R'))):
    MIN = 0
    MAX = MAXINT

    def __contains__(self, value):
        return self.L <= value <= self.R

    def __and__(self, other):
        L = max(self.L, other.L)
        R = min(self.R, other.R)
        return S(L, R) if L <= R else None

    def forward(self):
        return iter(range(self.L, self.R + 1))

    def backward(self):
        return iter(range(self.R, self.L - 1, -1))


class MemoryCachedDBRegistry(IterSeek):
    def __init__(self, connection, name, direction=Direction.F, inverted=False,
                 registry=None):

        self.inverted = inverted

        if registry is None:
            registry = Registry()

        if self.inverted:
            self.memory = RegistryIterSeek(~registry, direction=direction)
            self.db = ~DBRegistry(name, connection, direction=direction)
        else:
            self.memory = RegistryIterSeek(registry, direction=direction)
            self.db = DBRegistry(name, connection, direction=direction)

        self.seeked = True
        self.direction = direction
        self._iter = None

    def __contains__(self, value):
        return value in self.memory.registry or value in self.db

    @property
    def acked(self):
        return self.memory.registry.acked

    @property
    def add(self):
        return self.memory.registry.add

    def seek(self, pos):
        self.memory.seek(pos)
        self.db.seek(pos)
        self.seeked = True

    def __next__(self):
        if self.seeked:
            if self.inverted:
                self._iter = iter(self.memory & self.db)
            else:
                self._iter = iter(self.memory | self.db)

            self.seeked = False
        return next(self._iter)


def writefixture(conn, name):
    with conn.readers(write=True) as res:
        res.db[name] = res.env.open_db(key=name.encode('utf-8'), txn=res.txn)
        with RegistryDB.named(name).cursor(res) as cursor:
            cursor.put(20, 1)
            cursor.put(40, 30)
            cursor.put(150, 50)
            cursor.put(153, 152)


def console(conn, name):
    with conn.readers(write=True) as res:
        with RegistryDB.named(name).cursor(res) as cursor:
            import ipdb
            ipdb.set_trace()
            cursor


class BaseDBRegistry(IterSeek):
    def __init__(self, name, connection, direction=Direction.F):
        self.name = name
        self.conn = connection
        self.direction = direction
        self.last = None
        self._iter = None

        self.curr_s = None  # Current segment

    def _get_segment_by_pos(self, pos):
        raise NotImplementedError("Must be implemented in subclass.")

    def __contains__(self, value):
        segment = self._get_segment_by_pos(value)
        if segment is None:
            return False
        else:
            return segment.L <= value <= segment.R

    def seek(self, pos):
        segment = self._get_segment_by_pos(pos)
        if segment is None:
            return False
        else:
            if segment.L <= pos <= segment.R:
                if self.direction is Direction.F:
                    self._iter = iter(range(pos, segment.R + 1))
                else:
                    self._iter = iter(range(pos, segment.L - 1, -1))
            else:
                if self.direction is Direction.F:
                    self._iter = iter(range(segment.L, segment.R + 1))
                else:
                    self._iter = iter(range(segment.R, segment.L - 1, -1))

            return True

    def __next__(self):
        if self._iter is None:
            if self.direction is Direction.F:
                if not self.seek(0):
                    raise StopIteration
            else:
                if not self.seek(MAXINT):
                    raise StopIteration

        try:
            self.last = next(self._iter)
        except StopIteration:
            if self.direction is Direction.F:
                if not self.seek(self.last + 1):
                    raise StopIteration
            else:
                if not self.seek(self.last - 1):
                    raise StopIteration
            return self.__next__()
        else:
            return self.last


class IDBRegistry(BaseDBRegistry):
    """Inverted DBRegistry"""

    @MaskException(lmdb.ReadonlyError, ReaderDoesNotExist)
    def _get_segment_by_pos(self, pos):
        """
        Return the next segment given `pos` and self.direction.

        """
        with self.conn.readers(write=False) as res:
            with RegistryDB.named(self.name).cursor(res) as cursor:
                found = cursor.set_range(pos)
                if not found:
                    # Past the end of the database
                    if self.direction is Direction.F:
                        return S(pos, S.MAX)
                    else:
                        if cursor.last():
                            end, start = cursor.item()
                            return S(end + 1, pos)
                        else:
                            # Empty
                            return S(S.MIN, S.MAX)
                else:
                    end, start = cursor.item()
                    if start <= pos <= end:
                        # `pos` is in the segment
                        if self.direction is Direction.F:
                            if cursor.next():
                                s2_end, s2_start = cursor.item()
                                return S(end + 1, s2_start - 1)
                            else:
                                return S(end + 1, S.MAX)
                        else:
                            if cursor.prev():
                                s2_end, s2_start = cursor.item()
                                return S(s2_end + 1, start - 1)
                            else:
                                return S(S.MIN, start - 1)
                    else:
                        if self.direction is Direction.F:
                            return S(pos, start - 1)
                        else:
                            if cursor.prev():
                                end, start = cursor.item()
                                return S(end + 1, pos)
                            else:
                                return S(S.MIN, pos)

    def __invert__(self):
        return DBRegistry(self.name, self.conn, direction=self.direction)


class DBRegistry(BaseDBRegistry):
    @MaskException(lmdb.ReadonlyError, ReaderDoesNotExist)
    def _get_segment_by_pos(self, pos):
        """
        Return the next segment given `pos` and self.direction.

        """
        with self.conn.readers(write=False) as res:
            with RegistryDB.named(self.name).cursor(res) as cursor:
                found = cursor.set_range(pos)
                if not found:
                    # Past the end of the database
                    if self.direction is Direction.F:
                        return None
                    else:
                        if cursor.last():
                            end, start = cursor.item()
                            return S(start, end)
                        else:
                            # Empty
                            return None
                else:
                    end, start = cursor.item()
                    if start <= pos <= end:
                        # `pos` is in the segment
                        return S(start, end)
                    else:
                        if self.direction is Direction.F:
                            # Because set_range position the cursor in the next
                            # segment.
                            return S(start, end)
                        else:
                            if cursor.prev():
                                end, start = cursor.item()
                                return S(start, end)
                            else:
                                return None

    def __invert__(self):
        return IDBRegistry(self.name, self.conn, direction=self.direction)


class RegistryIterSeek(IterSeek):
    def __init__(self, registry, direction=Direction.F):
        self.registry = registry
        self.pos = None
        self.direction = direction

        self.last = None

        self.last_s = None
        self._curr_s = None

        if self.registry.acked:
            self.segments = zip(consume(self.walk_segments(),
                                        len(registry.acked) - 1),
                                self.walk_segments())
        else:
            self.segments = None

    @property
    def curr_s(self):
        return self._curr_s

    @curr_s.setter
    def curr_s(self, value):
        self._curr_s = value
        if isinstance(value, S):
            if self.direction is Direction.F:
                self._curr_s._iter = value.forward()
            else:
                self._curr_s._iter = value.backward()

    def walk_segments(self):
        if self.direction == Direction.F:
            return cycle(self.registry.acked)
        else:
            return cycle(reversed(self.registry.acked))

    def __next__(self):
        if not self.registry.acked:
            raise StopIteration
        elif self.pos is not None:
            pos, self.pos = self.pos, None

            if ((self.direction is Direction.F
                 and pos > self.registry.acked[-1].R)
                or (self.direction is Direction.B
                    and pos < self.registry.acked[0].L)):
                raise StopIteration
            else:
                for (self.last_s,  # pragma: no branch
                     self.curr_s) in self.segments:
                    # Ex: p = 10
                    if pos in self.curr_s:
                        # Sc(1 -- <p> -- 20)
                        if self.direction is Direction.F:
                            self.curr_s = S(pos, self.curr_s.R)
                        else:
                            self.curr_s = S(self.curr_s.L, pos)
                        return self.__next__()
                    elif self.direction is Direction.F:
                        # Sl < Sc < Sn | Except on edges
                        if self.last_s.R < pos < self.curr_s.L:
                            # Sl(1, 9) -- p> -- Sc(11, 20)
                            return self.__next__()
                        elif (pos not in self.last_s
                              and self.last_s.R > pos < self.curr_s.L
                              and self.last_s.R > self.curr_s.L):
                            # Sl(190, 200) -- p> -- Sc(20, 180)
                            return self.__next__()
                    else:
                        # Sl > Sc > Sn | Except on edges
                        if self.last_s.L > pos > self.curr_s.R:
                            return self.__next__()
                        elif (pos not in self.last_s
                              and self.last_s.L < pos > self.curr_s.R
                              and self.last_s.L < self.curr_s.R):
                            return self.__next__()
        else:
            if self.curr_s is None:
                self.last_s, self.curr_s = next(self.segments)

            try:
                n = next(self.curr_s._iter)
            except StopIteration:
                self.curr_s = None
                return self.__next__()
            else:
                if self.last is not None:
                    if self.direction is Direction.F and n < self.last:
                        raise StopIteration
                    elif self.direction is Direction.B and n > self.last:
                        raise StopIteration
                self.last = n
                return n

    def seek(self, pos):
        self.pos = pos
        self.last = None


class Registry:
    def __init__(self, acked=None):
        if acked is None:
            self.acked = []
        else:
            self.acked = acked

    def __iter__(self):
        def _iter():
            for segment in self.acked:
                yield from iter(range(segment.L, segment.R+1))
        return _iter()

    def add(self, idx):
        if not isinstance(idx, int):
            raise TypeError("idx must be int")
        elif not self.acked:
            self.acked.append(S(idx, idx))
            return True
        elif idx in self:
            return False
        else:
            for i, segment in enumerate(self.acked):
                left, right = segment.L, segment.R

                if right == idx - 1:
                    try:
                        if self.acked[i + 1].L == idx + 1:
                            right = self.acked[i + 1].R
                            del self.acked[i + 1]
                            self.acked[i] = S(left, right)
                            return True
                    except IndexError:
                        pass
                    self.acked[i] = S(left, idx)
                    return True
                elif left == idx + 1:
                    self.acked[i] = S(idx, right)
                    return True
            else:
                insort(self.acked, S(idx, idx))
                return True

    def __repr__(self):  # pragma: no cover
        return repr(self.acked)

    def __contains__(self, value):
        if not self.acked:
            return False

        idx = bisect_left(self.acked, (value, value))

        try:
            segment = self.acked[idx]
            left, right = segment.L, segment.R
        except IndexError:
            try:
                segment = self.acked[idx - 1]
                left, right = segment.L, segment.R
            except IndexError:  # pragma: no cover
                return False
            else:
                return left <= value <= right
        else:
            if left <= value <= right:
                return True
            elif idx > 0:
                segment = self.acked[idx - 1]
                left, right = segment.L, segment.R
                return left <= value <= right
            else:
                return False

    def __or__(self, other):
        a_ackd, b_ackd = self.acked.copy(), other.acked.copy()

        current_1 = current_2 = None
        new_acked = []
        while a_ackd or b_ackd:

            if current_1 is None:
                current_1 = popminleft(a_ackd, b_ackd)

            new_left = current_1.L

            current_2 = popminleft(a_ackd, b_ackd)
            if current_2 is None:
                break

            if (current_2.L - 1) <= current_1.R <= current_2.R:
                new_right = current_2.R
                current_1 = S(new_left, new_right)
            elif current_1.R < current_2.R:
                new_right = current_1.R
                new_acked.append(S(new_left, new_right))
                current_1 = current_2

        if current_1 is not None:
            new_acked.append(current_1)

        return Registry(acked=new_acked)

    def __and__(self, other):
        a_ackd, b_ackd = self.acked.copy(), other.acked.copy()

        current_1 = current_2 = None
        new_acked = []
        while a_ackd or b_ackd:
            if current_1 is None:
                current_1 = popminleft(a_ackd, b_ackd)
            current_2 = popminleft(a_ackd, b_ackd)
            if current_2 is None:
                break
            r = current_1 & current_2
            if r is not None:
                new_acked.append(r)
            current_1 = max(current_1, current_2, key=lambda s: s.R)
            current_2 = None

        return Registry(acked=new_acked)

    def __invert__(self):
        if not self.acked:
            return Registry([S(S.MIN, S.MAX)])
        else:
            new_acked = []

            if self.acked[0].L != S.MIN:
                new_acked.append(S(S.MIN, self.acked[0].L - 1))

            for a, b in zip(self.acked, self.acked[1:]):
                new_acked.append(S(a.R + 1, b.L - 1))

            if self.acked[-1].R != S.MAX:
                new_acked.append(S(self.acked[-1].R + 1, S.MAX))

            return Registry(new_acked)
