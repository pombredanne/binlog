from bisect import insort
from copy import deepcopy

from .binlog import Record


class Register:
    """
    reg = {
      1: [(1, 20), (30, 30)],
      2: [(2, 2)],
    }
    """
    def __init__(self, reg=None):
        if reg is not None:
            self.reg = deepcopy(reg)
        else:
            self.reg = {}

        self.liidx = 1
        self.clidx = 1

    def add(self, record, last=None):
        if self.reg == {}: 
            self.reg[record.liidx] = [(record.clidx, record.clidx)]
        elif not record.liidx in self.reg:
            self.reg[record.liidx] = [(record.clidx, record.clidx)]
        else:
            for idx, v in enumerate(self.reg[record.liidx]):
                l, r = v
                if r == (record.clidx - 1):
                    r = record.clidx
                    break
                elif l == (record.clidx + 1):
                    l = record.clidx
                    break
                elif last is None and l <= record.clidx <= r:
                    return None
            else:
                if last is None:
                    insort(self.reg[record.liidx],
                           (record.clidx, record.clidx))
                return None

            if last is not None and idx != last:
                if last > idx:
                    other = self.reg[record.liidx].pop(last)
                    current = self.reg[record.liidx].pop(idx)
                else:
                    current = self.reg[record.liidx].pop(idx)
                    other = self.reg[record.liidx].pop(last)
                if other[0] < current[0]:
                    insort(self.reg[record.liidx],
                           (other[0], current[1]))
                else:
                    insort(self.reg[record.liidx],
                           (current[0], other[1]))
            else:
                self.reg[record.liidx][idx] = (l, r)
                self.add(record, last=idx)

    def next_li(self):
        self.clidx = 1
        r = Record(self.liidx, self.clidx, None)
        self.liidx += 1
        return r

    def next_cl(self):
        r = Record(self.liidx, self.clidx, None)
        self.clidx += 1
        return r
