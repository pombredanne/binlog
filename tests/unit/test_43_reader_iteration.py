from itertools import zip_longest
from tempfile import TemporaryDirectory

from hypothesis import given
from hypothesis import strategies as st
import pytest


from binlog.databases import Entries
from binlog.model import Model


def test_reader_reads_bulk_create(tmpdir):
    with Model.open(tmpdir) as db:
        entries = [Model(idx=i) for i in range(10)]
        db.bulk_create(entries)

        db.register_reader('myreader')
        with db.reader('myreader') as reader:
            for current, expected in zip_longest(entries, reader):
                assert current == expected


@given(entries=st.lists(st.dictionaries(keys=st.text(), values=st.text())))
def test_reader_reads_create(entries):
    with TemporaryDirectory() as tmpdir:
        with Model.open(tmpdir) as db:
            for e in entries:
                db.create(**e)

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
