import pytest

from hypothesis import given, assume
from hypothesis import strategies as st


def test_segment_exists():
    try:
        from binlog.registry import S
    except ImportError as exc:
        assert False, exc


def test_segment_attributes():
    from binlog.registry import S

    s = S(0, 1)

    assert s.L == 0 
    assert s.R == 1


@given(a=st.integers(0, 10),
       b=st.integers(10, 20),
       c=st.integers(0, 10),
       d=st.integers(10, 20))
def test_segment_support_intersection_when_overlap(a, b, c, d):
    from binlog.registry import S

    s0 = S(a, b)
    s1 = S(c, d)

    res0 = s0 & s1
    res1 = s1 & s0
    
    assert res0 == res1 == S(max(a, c), min(b, d))


@given(a=st.integers(0, 10),
       b=st.integers(10, 20),
       c=st.integers(30, 40),
       d=st.integers(40, 50))
def test_segment_support_intersection_when_no_overlap(a, b, c, d):
    from binlog.registry import S

    s0 = S(a, b)
    s1 = S(c, d)

    res0 = s0 & s1
    res1 = s1 & s0
    
    assert res0 == res1 == None
