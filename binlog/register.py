from copy import deepcopy


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
                    self.reg[record.liidx].append((record.clidx, record.clidx))
                return None

            if last is not None and idx != last:
                if last > idx:
                    other = self.reg[record.liidx].pop(last)
                    current = self.reg[record.liidx].pop(idx)
                else:
                    current = self.reg[record.liidx].pop(idx)
                    other = self.reg[record.liidx].pop(last)
                if other[0] < current[0]:
                    self.reg[record.liidx].append((other[0], current[1]))
                else:
                    self.reg[record.liidx].append((current[0], other[1]))
            else:
                self.reg[record.liidx][idx] = (l, r)
                self.add(record, last=idx)
