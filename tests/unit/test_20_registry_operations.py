from hypothesis import strategies as st
from hypothesis import given
import pytest

from binlog.registry import Registry, S


def test_registry_empty_invert():
    r = ~Registry()
    assert r.acked == [S(S.MIN, S.MAX)]


@given(a=st.integers(min_value=S.MIN, max_value=S.MAX),
       b=st.integers(min_value=S.MIN, max_value=S.MAX))
def test_registry_onesegment_invert(a, b):
    start, end = min(a, b), max(a, b)

    r = ~Registry([S(start, end)])

    if start == S.MIN:
        if end == S.MIN:
            assert r.acked == [S(start + 1, S.MAX)]
        elif end == S.MAX:
            assert r.acked == []
    elif end == S.MAX:
        assert r.acked == [S(S.MIN, end - 1)]
    else:
        assert r.acked == [S(S.MIN, start - 1), S(end + 1, S.MAX)]


@given(values=st.lists(st.integers(min_value=S.MIN, max_value=S.MAX)))
def test_registry_multiplesegment_invert(values):
    r = Registry()

    for point in values:
        r.add(point)

    assert (~~r).acked == r.acked
