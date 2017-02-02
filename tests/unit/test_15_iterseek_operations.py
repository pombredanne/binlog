from functools import reduce
import operator as op

from hypothesis import given, assume, example
from hypothesis import strategies as st
import pytest

from binlog.abstract import Direction

def check_resiter(resiter, expected, direction, seeks):
    """
    Check a resiter against a list of integers.

    """
    # Initial
    assert list(resiter) == list(sorted(expected,
                                        reverse=direction is Direction.B))

    # Seeks
    for s in seeks:
        resiter.seek(s)
        if direction is Direction.F:
            assert list(resiter) == list(sorted(
                [i for i in expected if i>=s]))
        else:
            assert list(resiter) == list(sorted(
                [i for i in expected if i<=s], reverse=True))



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
       b=st.sets(st.integers(min_value=0, max_value=100)),
       seeks=st.sets(st.integers(min_value=0, max_value=100)))
def test_anditerseek_operation(dummyiterseek, direction, a, b, seeks):
    from binlog.operations import ANDIterSeek

    resiter = ANDIterSeek(dummyiterseek(a, direction=direction),
                          dummyiterseek(b, direction=direction))
    expected = a & b

    check_resiter(resiter, expected, direction, seeks)


@pytest.mark.parametrize("direction", Direction)
@given(values=st.lists(st.sets(st.integers(min_value=0, max_value=100)),
                       min_size=3),
       seeks=st.sets(st.integers(min_value=0, max_value=100)))
def test_anditerseek_multiple(dummyiterseek, direction, values, seeks):
    iterseeks = []
    for vlist in values:
        iterseeks.append(dummyiterseek(vlist, direction=direction))

    resiter = reduce(op.and_, iterseeks)
    expected = reduce(op.and_, values)

    check_resiter(resiter, expected, direction, seeks)


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
       b=st.sets(st.integers(min_value=0, max_value=100)),
       seeks=st.sets(st.integers(min_value=0, max_value=100)))
def test_oriterseek_operation(dummyiterseek, direction, a, b, seeks):
    from binlog.operations import ORIterSeek

    resiter = ORIterSeek(dummyiterseek(a, direction=direction),
                         dummyiterseek(b, direction=direction))
    expected = a | b

    check_resiter(resiter, expected, direction, seeks)


@pytest.mark.parametrize("direction", Direction) 
@given(values=st.lists(st.sets(st.integers(min_value=0, max_value=100)),
                       min_size=3),
       seeks=st.sets(st.integers(min_value=0, max_value=100)))
def test_oriterseek_multiple(dummyiterseek, direction, values, seeks):
    iterseeks = []
    for vlist in values:
        iterseeks.append(dummyiterseek(vlist, direction=direction))

    resiter = reduce(op.or_, iterseeks)
    expected = reduce(op.or_, values)

    check_resiter(resiter, expected, direction, seeks)


#
# COMPOSED
#
@pytest.mark.parametrize("direction", Direction)
@given(a=st.sets(st.integers(min_value=0, max_value=100)),
       b=st.sets(st.integers(min_value=0, max_value=100)),
       c=st.sets(st.integers(min_value=0, max_value=100)),
       d=st.sets(st.integers(min_value=0, max_value=100)),
       seeks=st.sets(st.integers(min_value=0, max_value=100)))
def test_anditerseek_oriterseek_composed_1(dummyiterseek, direction,
                                           a, b, c, d, seeks):
    resiter = ((dummyiterseek(a, direction=direction)
                | dummyiterseek(b, direction=direction))
               & (dummyiterseek(c, direction=direction)
                  | dummyiterseek(d, direction=direction)))
    expected = (a | b) & (c | d)

    check_resiter(resiter, expected, direction, seeks)


@pytest.mark.parametrize("direction", Direction)
@given(a=st.sets(st.integers(min_value=0, max_value=100)),
       b=st.sets(st.integers(min_value=0, max_value=100)),
       c=st.sets(st.integers(min_value=0, max_value=100)),
       d=st.sets(st.integers(min_value=0, max_value=100)),
       seeks=st.sets(st.integers(min_value=0, max_value=100)))
def test_anditerseek_oriterseek_composed_2(dummyiterseek, direction,
                                           a, b, c, d, seeks):
    resiter = ((dummyiterseek(a, direction=direction)
                & dummyiterseek(b, direction=direction))
               | (dummyiterseek(c, direction=direction)
                  & dummyiterseek(d, direction=direction)))
    expected = (a & b) | (c & d)

    check_resiter(resiter, expected, direction, seeks)
