from itertools import zip_longest

import pytest

from binlog.databases import Entries
from binlog.model import Model


def test_reader_reads(tmpdir):
    with Model.open(tmpdir) as db:
        entries = [Model(idx=i) for i in range(10)]
        db.bulk_create(entries)

        db.register_reader('myreader')
        with db.reader('myreader') as reader:
            for current, expected in zip_longest(entries, reader):
                assert current == expected


def test_reader_reversed_read(tmpdir):
    with Model.open(tmpdir) as db:
        entries = [Model(idx=i) for i in range(10)]
        db.bulk_create(entries)

        db.register_reader('myreader')
        with db.reader('myreader') as reader:
            for current, expected in zip_longest(reversed(entries),
                                                 reversed(reader)):
                assert current == expected


def test_empty_iterator(tmpdir):
    with Model.open(tmpdir) as db:
        # Create and delete one register
        db.create(test="data")
        with db.data(write=True) as res:
            with Entries.cursor(res) as cursor:
                assert cursor.pop(0)

        db.register_reader('myreader')
        with db.reader('myreader') as reader:
            for _ in reader:
                assert False, "SHOULD be empty"


def test_empty_reverse_iterator(tmpdir):
    with Model.open(tmpdir) as db:
        db.register_reader('myreader')

        # Create and delete one register
        db.create(test="data")
        with db.data(write=True) as res:
            with Entries.cursor(res) as cursor:
                assert cursor.pop(0)

        with db.reader('myreader') as reader:
            for _ in reversed(reader):
                assert False, "SHOULD be empty"
