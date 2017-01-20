from functools import wraps


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

