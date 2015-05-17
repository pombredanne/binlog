from binlog.writer import Writer

from random import randint

w = Writer('test')

while True:
    w.append("Hello World!")
    print('.', end='', flush=True)
