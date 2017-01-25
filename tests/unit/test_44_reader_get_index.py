import pytest

from binlog.model import Model


def test_reader_index(tmpdir):
    with Model.open(tmpdir) as db:
        entries = [Model(idx=i) for i in range(10)]
        db.bulk_create(entries)

        db.register_reader('myreader')
        with db.reader('myreader') as reader:
            for e in entries:
                assert e == reader[e['idx']]


def test_reader_non_integer(tmpdir):
    with Model.open(tmpdir) as db:
        db.register_reader('myreader')
        with db.reader('myreader') as reader:
            with pytest.raises(TypeError):
                reader[None]


def test_reader_unknown_index(tmpdir):
    with Model.open(tmpdir) as db:
        db.create(test='test')

        db.register_reader('myreader')
        with db.reader('myreader') as reader:
            with pytest.raises(IndexError):
                reader[1]


def test_reader_unknown_negative_index(tmpdir):
    with Model.open(tmpdir) as db:
        db.create(test='test')

        db.register_reader('myreader')
        with db.reader('myreader') as reader:
            with pytest.raises(IndexError):
                reader[-2]


def test_reader_negative_index(tmpdir):
    with Model.open(tmpdir) as db:
        entries = [Model(idx=i) for i in range(10)]
        db.bulk_create(entries)

        db.register_reader('myreader')
        with db.reader('myreader') as reader:
            positive_indexes = range(len(entries))
            negative_indexes = [(1 + i) * -1
                                for i in reversed(positive_indexes)]


            for pos, neg in zip(positive_indexes, negative_indexes):
                assert reader[pos] == reader[neg]


