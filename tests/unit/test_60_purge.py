from functools import reduce
from tempfile import TemporaryDirectory
import operator as op

from hypothesis import given, example, settings
from hypothesis import strategies as st
import pytest

from binlog.model import Model


def test_purge_without_readers(tmpdir):

    with Model.open(tmpdir) as db:
        db.create(test='data')

        removed, not_found = db.purge()

        assert removed == not_found == 0


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

            removed, not_found = db.purge()
            assert removed == len(acked)
            assert not_found == 0

            with db.reader() as reader:
                for pk in range(100):
                    if pk in acked:
                        with pytest.raises(IndexError):
                            reader[pk]
                    else:
                        assert reader[pk]


@given(acked_list=st.lists(st.sets(st.integers(min_value=0, max_value=99)),
                           min_size=1))
@settings(deadline=None)
@example(acked_list=[{0}, set()])
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

            removed, not_found = db.purge()
            assert removed == len(common)
            assert not_found == 0

            with db.reader() as reader:
                for pk in range(100):
                    if pk in common:
                        with pytest.raises(IndexError):
                            reader[pk]
                    else:
                        assert reader[pk]


@given(acked=st.sets(st.integers(min_value=0, max_value=99)))
def test_purge_with_not_found(acked):
    from binlog.databases import Entries

    with TemporaryDirectory() as tmpdir:
        with Model.open(tmpdir) as db:
            entries = [Model(idx=i) for i in range(100)]
            db.bulk_create(entries)

            db.register_reader('myreader')
            with db.reader('myreader') as reader:
                for pk in acked:
                    reader.ack(reader[pk])

            # Delete everything
            with db.data(write=True) as res:
                with Entries.cursor(res) as cursor:
                    for pk in range(100):
                        cursor.pop(pk)

            removed, not_found = db.purge(chunk_size=10)

            assert removed == 0

            # Because purge now uses ANDS the Entries cursor with
            # `common_acked`, "not_found" always will be 0.
            assert not_found == 0


def test_purge_chunk_size_min_size(tmpdir):
    with Model.open(tmpdir) as db:
        with pytest.raises(ValueError):
            db.purge(chunk_size=0)
