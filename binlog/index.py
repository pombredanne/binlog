from .database import Database
from .serializer import NumericSerializer
from .serializer import TextSerializer


class Index(Database):
    V = NumericSerializer


class TextIndex(Index):
    K = TextSerializer


class NumericIndex(Index):
    K = NumericSerializer
