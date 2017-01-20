from .abstract import Database
from .index import NumericIndex

from .serializer import NumericSerializer
from .serializer import ObjectSerializer
from .serializer import TextSerializer


class Config(Database):
    K = TextSerializer
    V = ObjectSerializer


class Entries(NumericIndex):
    V = ObjectSerializer


class Checkpoints(Database):
    K = TextSerializer
    V = ObjectSerializer
