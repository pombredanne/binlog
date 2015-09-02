from binlog.writer import TDSWriter

from random import randint

w = TDSWriter('test')

while True:
    w.append("Hello World!")
    print('.', end='', flush=True)
