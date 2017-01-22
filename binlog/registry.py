from bisect import insort, bisect_left
from collections import deque
from itertools import count


class Registry:
    def __init__(self, acked=None):
        self.initial = None
        if acked is None:
            self.acked = []
        else:
            self.acked = acked

    def add(self, idx):
        if not isinstance(idx, int):
            raise ValueError("idx must be int")

        if self.initial is None:
            self.initial = idx

        if not self.acked:
            self.acked.append((idx, idx))
            return True
        elif idx in self:
            return False
        else:
            for i, segment in enumerate(self.acked):
                left, right = segment 

                if right == idx - 1:
                    try:
                        if self.acked[i + 1][0] == idx + 1:
                            _, right = self.acked.pop(i + 1)
                            self.acked[i] = (left, right)
                            return True
                    except IndexError:
                        pass
                    self.acked[i] = (left, idx)
                    return True
                elif left == idx + 1:
                    self.acked[i] = (idx, right)
                    return True
                # elif left <= idx <= right:
                #     return False
            else:
                insort(self.acked, (idx, idx))
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

    def __add__(self, other):
        a_ackd = deque(self.acked)
        b_ackd = deque(other.acked)

        def popminleft():
            if a_ackd and b_ackd:
                if a_ackd[0][0] <= b_ackd[0][0]:
                    return a_ackd.popleft()
                else:
                    return b_ackd.popleft()
            elif a_ackd:
                return a_ackd.popleft()
            elif b_ackd:
                return b_ackd.popleft()
            else:
                return None

        current_1 = None
        new_acked = []
        while a_ackd or b_ackd:

            if current_1 is None:
                current_1 = popminleft()

            # current_1 can't be None
            current_1_left, current_1_right = current_1 

            new_left = current_1_left

            current_2 = popminleft()
            if current_2 is None:
                new_acked.append(current_1)
                break
            else:
                current_2_left, current_2_right = current_2

            if (current_2_left - 1) <= current_1_right <= current_2_right:
                new_right = current_2_right
                current_1 = (new_left, new_right)
            elif current_1_right < current_2_right:
                new_right = current_1_right
                new_acked.append((new_left, new_right))
                current_1 = current_2

        if current_1 is not None:
            new_acked.append(current_1)

        return Registry(acked=new_acked)
