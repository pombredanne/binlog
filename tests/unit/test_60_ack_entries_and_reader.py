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
