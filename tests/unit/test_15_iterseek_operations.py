from functools import reduce
import operator as op

from hypothesis import given, assume, example
from hypothesis import strategies as st
import pytest

from binlog.abstract import Direction

#
# BASE
#
def test_binaryiterseek_exists():
    try:
        from binlog.operations import BinaryIterSeek
    except ImportError as exc:
        assert False, exc


def test_binaryiterseek_different_directions(dummyiterseek):
    from binlog.operations import BinaryIterSeek

    class DummyBinaryIterSeek(BinaryIterSeek):
        def seek(self, value):
            pass
        def __next__(self):
            pass

    with pytest.raises(ValueError):
        DummyBinaryIterSeek(dummyiterseek([], direction=Direction.F),
                            dummyiterseek([], direction=Direction.B))


#
# AND
#
def test_anditerseek_exists():
    try:
        from binlog.operations import ANDIterSeek
    except ImportError as exc:
        assert False, exc


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
                       min_size=3))
def test_anditerseek_multiple(dummyiterseek, direction, values):
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


#
# OR
#
def test_oriterseek_exists():
    try:
        from binlog.operations import ORIterSeek
    except ImportError as exc:
        assert False, exc


@pytest.mark.parametrize("direction", Direction) 
@given(a=st.sets(st.integers(min_value=0, max_value=100)),
       b=st.sets(st.integers(min_value=0, max_value=100)))
def test_oriterseek_operation(dummyiterseek, direction, a, b):
    from binlog.operations import ORIterSeek

    resiter = ORIterSeek(dummyiterseek(a, direction=direction),
                         dummyiterseek(b, direction=direction))

    expected = a | b

    current = list(resiter)

    for v in expected:
        assert v in current


@pytest.mark.parametrize("direction", Direction) 
@given(values=st.lists(st.sets(st.integers(min_value=0, max_value=100)),
                       min_size=3))
def test_oriterseek_multiple(dummyiterseek, direction, values):
    iterseeks = []
    for vlist in values:
        iterseeks.append(dummyiterseek(vlist, direction=direction))

    resiter = reduce(op.or_, iterseeks)

    expected = reduce(op.or_, values)

    current = list(resiter)

    for v in expected:
        assert v in current


#
# COMPOSED
#
@pytest.mark.parametrize("direction", Direction)
@given(a=st.sets(st.integers(min_value=0, max_value=100)),
       b=st.sets(st.integers(min_value=0, max_value=100)),
       c=st.sets(st.integers(min_value=0, max_value=100)),
       d=st.sets(st.integers(min_value=0, max_value=100)))
@example(a=set(), b={1}, c={0}, d={1})
def test_anditerseek_oriterseek_composed_1(dummyiterseek, direction,
                                           a, b, c, d):

    direction = Direction.B

    direction = Direction.B
    resiter = ((dummyiterseek(a, direction=direction)
                | dummyiterseek(b, direction=direction))
               & (dummyiterseek(c, direction=direction)
                  | dummyiterseek(d, direction=direction)))

    expected = (a | b) & (c | d)
    non_expected = (a | b | c | d) - expected

    current = list(resiter)

    for v in expected:
        assert v in current

    for v in non_expected:
        assert v not in current


@pytest.mark.parametrize("direction", Direction)
@given(a=st.sets(st.integers(min_value=0, max_value=100)),
       b=st.sets(st.integers(min_value=0, max_value=100)),
       c=st.sets(st.integers(min_value=0, max_value=100)),
       d=st.sets(st.integers(min_value=0, max_value=100)))
def test_anditerseek_oriterseek_composed_2(dummyiterseek, direction,
                                           a, b, c, d):
    resiter = ((dummyiterseek(a, direction=direction)
                & dummyiterseek(b, direction=direction))
               | (dummyiterseek(c, direction=direction)
                  & dummyiterseek(d, direction=direction)))

    expected = (a & b) | (c & d)
    non_expected = (a | b | c | d) - expected

    current = list(resiter)

    for v in expected:
        assert v in current

    for v in non_expected:
        assert v not in current
