from .abstract import Database
from .index import NumericIndex


class Config(Database):
    K = TextSerializer
    V = ObjectSerializer


class Entries(NumericIndex):
    V = ObjectSerializer


class Checkpoints(Database):
    K = TextSerializer
    V = ObjectSerializer
