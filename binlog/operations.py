from binlog.abstract import IterSeek, Direction


class ANDIterSeek(IterSeek):
    def __init__(self, a, b):
        self.a = a
        self.b = b

        if self.a.direction != self.b.direction:
            raise ValueError("both iterseeks must have the same direction")

        self.direction = self.a.direction

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
