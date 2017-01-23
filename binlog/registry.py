from bisect import insort, bisect_left
from collections import deque, namedtuple
from itertools import count

from .util import popminleft


class S(namedtuple('Segment', 'L,R')):
    def __and__(self, other):
        L = max(self.L, other.L)
        R = min(self.R, other.R)
        return S(L, R) if L<=R else None


class Registry:
    def __init__(self, acked=None):
        self.initial = None
        if acked is None:
            self.acked = deque()
        else:
            self.acked = acked

    def add(self, idx):
        if not isinstance(idx, int):
            raise ValueError("idx must be int")

        if self.initial is None:
            self.initial = idx

        if not self.acked:
            self.acked.append(S(idx, idx))
            return True
        elif idx in self:
            return False
        else:
            for i, segment in enumerate(self.acked):
                left, right = segment 

                if right == idx - 1:
                    try:
                        if self.acked[i + 1].L == idx + 1:
                            _, right = self.acked[i + 1]
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
            left, right = self.acked[idx]
        except IndexError:
            try:
                left, right = self.acked[idx - 1]
            except IndexError:  # pragma: no cover
                return False
            else:
                return left <= value <= right
        else:
            if left <= value <= right:
                return True
            elif idx > 0:
                left, right = self.acked[idx - 1]
                return left <= value <= right
            else:
                return False

    def __or__(self, other):
        a_ackd = self.acked.copy()
        b_ackd = other.acked.copy()

        current_1 = None
        new_acked = deque()
        while a_ackd or b_ackd:

            if current_1 is None:
                current_1 = popminleft(a_ackd, b_ackd)

            # current_1 can't be None
            current_1_left, current_1_right = current_1 

            new_left = current_1_left

            current_2 = popminleft(a_ackd, b_ackd)
            if current_2 is None:
                new_acked.append(current_1)
                break
            else:
                current_2_left, current_2_right = current_2

            if (current_2_left - 1) <= current_1_right <= current_2_right:
                new_right = current_2_right
                current_1 = S(new_left, new_right)
            elif current_1_right < current_2_right:
                new_right = current_1_right
                new_acked.append(S(new_left, new_right))
                current_1 = current_2

        if current_1 is not None:
            new_acked.append(current_1)

        return Registry(acked=new_acked)

    def __add__(self, other):
        pass
