from functools import wraps
from itertools import islice
import collections


class MaskException:
    def __init__(self, exp, mask):
        self.exp = exp
        self.mask = mask

    def __call__(self, f):
        @wraps(f)
        def wrapper(*args, **kwds):
            try:
                return f(*args, **kwds)
            except self.exp as exc:
                raise self.mask from exc
        return wrapper

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is self.exp:
            raise self.mask from exc_type


def popminleft(a, b):
    if a and b:
        if a[0].L <= b[0].L:
            return a.pop(0)
        else:
            return b.pop(0)
    elif a:
        return a.pop(0)
    elif b:
        return b.pop(0)
    else:
        return None


def cmp(a, b):                                                              
    """ http://codegolf.stackexchange.com/a/49779 """                       
    return (a > b) - (a < b)


def consume(iterator, n):  # pragma: no cover
    """
    Advance the iterator n-steps ahead. If n is none, consume entirely.

    https://docs.python.org/3.6/library/itertools.html#itertools-recipes

    """
    # Use functions that consume iterators at C speed.
    if n is None:
        # feed the entire iterator into a zero-length deque
        collections.deque(iterator, maxlen=0)
    else:
        # advance to the empty slice starting at position n
        next(islice(iterator, n, n), None)

    return iterator
