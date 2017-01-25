from .abstract import Database
from .serializer import DatetimeSerializer
from .serializer import NumericSerializer
from .serializer import TextSerializer


class Index(Database):
    V = NumericSerializer

    def __init__(self, *args, mandatory=True, **kwargs):
        self.mandatory = mandatory
        super().__init__(*args, **kwargs)


class TextIndex(Index):
    K = TextSerializer


class NumericIndex(Index):
    K = NumericSerializer


class DatetimeIndex(Index):
    K = DatetimeSerializer
