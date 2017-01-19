from collections import namedtuple
from bisect import insort, bisect_left


class Register:
    def __init__(self):
        self.initial = None
        self.acked = []

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

    def __repr__(self):
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
            except IndexError:
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
