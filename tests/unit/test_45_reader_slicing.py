import pytest
from itertools import product

from binlog.model import Model


@pytest.mark.parametrize("start", list(range(4)))
def test_slice_positive_start_no_stop_no_step(tmpdir, start):

    with Model.open(tmpdir) as db:
        entries = [Model(idx=i) for i in range(4)]
        db.bulk_create(entries)

        with db.reader() as reader:
            expected = list(entries[start::])
            current = list(reader[start::])
            assert expected == current


@pytest.mark.parametrize("start,stop", product(range(4), range(4)))
def test_slice_positive_start_stop_no_step(tmpdir, start, stop):

    with Model.open(tmpdir) as db:
        entries = [Model(idx=i) for i in range(4)]
        db.bulk_create(entries)

        with db.reader() as reader:
            expected = list(entries[start:stop:])
            current = list(reader[start:stop:])
            assert expected == current


def test_slice_step_cannot_be_zero(tmpdir):
    with Model.open(tmpdir) as db:
        entries = [Model(idx=i) for i in range(4)]
        db.bulk_create(entries)

        with db.reader() as reader:
            with pytest.raises(ValueError):
                reader[::0]


@pytest.mark.parametrize("start,stop,step",
                         product(range(4), range(4), range(1, 4)))
def test_slice_positive_start_stop_step(tmpdir, start, stop, step):

    with Model.open(tmpdir) as db:
        entries = [Model(idx=i) for i in range(4)]
        db.bulk_create(entries)

        with db.reader() as reader:
            expected = list(entries[start:stop:step])
            current = list(reader[start:stop:step])
            assert expected == current


@pytest.mark.parametrize("start", list(range(-6, 0)))
def test_slice_negative_start_no_stop_no_step(tmpdir, start):
    with Model.open(tmpdir) as db:
        entries = [Model(idx=i) for i in range(4)]
        db.bulk_create(entries)

        with db.reader() as reader:
            expected = list(entries[start::])
            current = list(reader[start::])
            assert expected == current


@pytest.mark.parametrize("start,stop", product(range(-6, 0), range(-6, 0)))
def test_slice_negative_start_stop_no_step(tmpdir, start, stop):
    with Model.open(tmpdir) as db:
        entries = [Model(idx=i) for i in range(4)]
        db.bulk_create(entries)

        with db.reader() as reader:
            expected = list(entries[start:stop:])
            current = list(reader[start:stop:])
            assert expected == current


@pytest.mark.parametrize("start,stop,step",
                         product(range(-6, 0), range(-6, 0), range(-6, -1)))
def test_slice_negative_start_stop_step(tmpdir, start, stop, step):
    with Model.open(tmpdir) as db:
        entries = [Model(idx=i) for i in range(4)]
        db.bulk_create(entries)

        with db.reader() as reader:
            expected = list(entries[start:stop:step])
            current = list(reader[start:stop:step])
            assert expected == current
