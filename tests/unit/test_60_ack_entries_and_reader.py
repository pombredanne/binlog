from hypothesis import given
from hypothesis import strategies as st
import pytest

from binlog.model import Model
from tempfile import TemporaryDirectory


@given(acks=st.lists(st.integers(min_value=0, max_value=99)))
def test_reader_reads_except_acked(acks):
    with TemporaryDirectory() as tmpdir:
        with Model.open(tmpdir) as db:
            entries = [Model(idx=i) for i in range(100)]
            db.bulk_create(entries)

            db.register_reader('myreader')
            with db.reader('myreader') as reader:
                for pk in acks:
                    reader.ack(reader[pk])

                expected = set(range(100)) - set(acks)
                current = {e.pk for e in reader}

                assert expected == current


@given(acks=st.lists(st.integers(min_value=0, max_value=99)))
def test_reader_reversed_reads_except_acked(acks):
    with TemporaryDirectory() as tmpdir:
        with Model.open(tmpdir) as db:
            entries = [Model(idx=i) for i in range(100)]
            db.bulk_create(entries)

            db.register_reader('myreader')
            with db.reader('myreader') as reader:
                for pk in acks:
                    reader.ack(reader[pk])

                expected = set(range(100)) - set(acks)
                current = {e.pk for e in reversed(reader)}

                assert expected == current


def test_multiple_instances_of_same_reader_can_ack_safe1(tmpdir):
    with Model.open(tmpdir) as db:
        e0 = db.create(test='data0')
        e1 = db.create(test='data1')

        db.register_reader('myreader')
        with db.reader('myreader') as reader1:
            reader1.ack(e0)
            with db.reader('myreader') as reader2:
                reader2.ack(e1)

        with db.reader('myreader') as reader:
            assert not list(reader)


def test_multiple_instances_of_same_reader_can_ack_safe2(tmpdir):
    with Model.open(tmpdir) as db:
        e0 = db.create(test='data0')
        e1 = db.create(test='data1')

        db.register_reader('myreader')
        with db.reader('myreader') as reader1:
            with db.reader('myreader') as reader2:
                reader2.ack(e1)
            reader1.ack(e0)

        with db.reader('myreader') as reader:
            assert not list(reader)


def test_multiple_instances_of_same_reader_can_ack_safe3(tmpdir):
    with Model.open(tmpdir) as db:
        e0 = db.create(test='data0')
        e1 = db.create(test='data1')

        db.register_reader('myreader')
        with db.reader('myreader') as reader1:
            with db.reader('myreader') as reader2:
                reader1.ack(e0)
                reader2.ack(e1)

        with db.reader('myreader') as reader:
            assert not list(reader)


def test_multiple_instances_of_same_reader_can_ack_safe4(tmpdir):
    with Model.open(tmpdir) as db:
        e0 = db.create(test='data0')
        e1 = db.create(test='data1')

        db.register_reader('myreader')
        with db.reader('myreader') as reader1:
            with db.reader('myreader') as reader2:
                reader2.ack(e1)
                reader1.ack(e0)

        with db.reader('myreader') as reader:
            assert not list(reader)


def test_ack_is_not_effective_before_commit(tmpdir):
    with Model.open(tmpdir) as db:
        e0 = db.create(test='data0')

        db.register_reader('myreader')
        with db.reader('myreader') as reader1:
            reader1.ack(e0)
            with db.reader('myreader') as reader2:
                assert list(reader2)


def test_ack_is_effective_after_commit(tmpdir):
    with Model.open(tmpdir) as db:
        e0 = db.create(test='data0')

        db.register_reader('myreader')
        with db.reader('myreader') as reader1:
            reader1.ack(e0)
            reader1.commit()
            with db.reader('myreader') as reader2:
                assert not list(reader2)
