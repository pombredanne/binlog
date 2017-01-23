from functools import reduce
from tempfile import TemporaryDirectory
import operator as op

from hypothesis import given, example
from hypothesis import strategies as st
import pytest

from binlog.model import Model


def test_purge_without_readers(tmpdir):

    with Model.open(tmpdir) as db:
        db.create(test='data')

        removed, errors = db.purge()

        assert removed == errors == 0


@given(acked=st.sets(st.integers(min_value=0, max_value=99)))
def test_purge_with_one_reader(acked):
    with TemporaryDirectory() as tmpdir:
        with Model.open(tmpdir) as db:
            entries = [Model(idx=i) for i in range(100)]
            db.bulk_create(entries)

            db.register_reader('myreader')
            with db.reader('myreader') as reader:
                for pk in acked:
                    reader.ack(reader[pk])

            removed, errors = db.purge()
            assert removed == len(acked)
            assert errors == 0

            with db.reader() as reader:
                for pk in range(100):
                    if pk in acked:
                        with pytest.raises(IndexError):
                            reader[pk]
                    else:
                        assert reader[pk]


@given(acked_list=st.lists(st.sets(st.integers(min_value=0, max_value=99)),
                           min_size=1,
                           average_size=10))
@example(acked_list = [{0}, set()])
def test_purge_with_multiple_reader(acked_list):
    with TemporaryDirectory() as tmpdir:
        with Model.open(tmpdir) as db:
            entries = [Model(idx=i) for i in range(100)]
            db.bulk_create(entries)

            for i, acked in enumerate(acked_list):
                db.register_reader('myreader_%d' % i)
                with db.reader('myreader_%d' % i) as reader:
                    for pk in acked:
                        reader.ack(reader[pk])

            common = reduce(op.and_, acked_list) 

            removed, errors = db.purge()
            assert removed == len(common)
            assert errors == 0

            with db.reader() as reader:
                for pk in range(100):
                    if pk in common:
                        with pytest.raises(IndexError):
                            reader[pk]
                    else:
                        assert reader[pk]
