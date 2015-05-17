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

        self.liidx = self.clidx = 0

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
                current = self.reg[record.liidx].pop(idx)
                other = self.reg[record.liidx].pop(last)
                insort(self.reg[record.liidx],
                       (other[0], current[1]))
            else:
                self.reg[record.liidx][idx] = (l, r)
                self.add(record, last=idx)

    def next_cl(self):
        if self.liidx == 0:
            self.liidx = 1
        self.clidx += 1
        r = Record(self.liidx, self.clidx, None)
        return r

    def next_li(self):
        if self.liidx == 0:
            self.liidx = 1
        else:
            self.liidx += 1
        self.clidx = 0
        return self.next_cl()

    def reset(self):
        """Resets liidx & clidx."""
        self.liidx = 0
        self.clidx = 0

    def next(self, log=False):
        """This method return the next record not in self.reg."""
        def get_next(i, l):
            for l, r in l:
                if l <= i <= r:
                    return r+1
                elif l > i:
                    break
            return i

        if log:
            r = self.next_li()
        else:
            r = self.next_cl()

        n = get_next(r.clidx, self.reg.get(self.liidx, []))
        self.clidx = n
        return Record(self.liidx, self.clidx, None)
