from functools import reduce
import operator as op

from hypothesis import given, assume, example
from hypothesis import strategies as st
import pytest

from binlog.abstract import Direction


def test_anditerseek_exists():
    try:
        from binlog.operations import ANDIterSeek
    except ImportError as exc:
        assert False, exc


def test_anditerseek_different_directions(dummyiterseek):
    from binlog.operations import ANDIterSeek

    with pytest.raises(ValueError):
        ANDIterSeek(dummyiterseek([], direction=Direction.F),
                    dummyiterseek([], direction=Direction.B))


@pytest.mark.parametrize("direction", Direction) 
@given(a=st.sets(st.integers(min_value=0, max_value=100)),
       b=st.sets(st.integers(min_value=0, max_value=100)))
def test_anditerseek_operation(dummyiterseek, direction, a, b):
    from binlog.operations import ANDIterSeek

    resiter = ANDIterSeek(dummyiterseek(a, direction=direction),
                          dummyiterseek(b, direction=direction))

    expected = a & b
    non_expected = expected - (a | b)

    current = list(resiter)

    for v in expected:
        assert v in current
    for v in non_expected:
        assert v not in current


@pytest.mark.parametrize("direction", Direction) 
@given(values=st.lists(st.sets(st.integers(min_value=0, max_value=100)),
                       min_size=1))
def test_iterseek_and_multiple(dummyiterseek, direction, values):
    iterseeks = []
    for vlist in values:
        iterseeks.append(dummyiterseek(vlist, direction=direction))

    resiter = reduce(op.and_, iterseeks)

    expected = reduce(op.and_, values)
    non_expected = expected - reduce(op.or_, values)

    current = list(resiter)

    for v in expected:
        assert v in current
    for v in non_expected:
        assert v not in current
