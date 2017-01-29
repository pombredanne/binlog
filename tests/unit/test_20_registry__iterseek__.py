from itertools import chain

from hypothesis import given, assume, example
from hypothesis import strategies as st
import pytest

from binlog.registry import Registry, S
from binlog.abstract import Direction


def test_registryiterseek_exists():
    try:
        from binlog.registry import RegistryIterSeek
    except ImportError as exc:
        assert False, exc


#
# FORWARD
#
def test_registryiterseek_forward_empty_unset():
    from binlog.registry import RegistryIterSeek

    r = RegistryIterSeek(Registry())
    assert not list(r)


@given(seek_to=st.integers(min_value=0, max_value=2**64-1))
def test_registryiterseek_forward_empty_seek(seek_to):
    from binlog.registry import RegistryIterSeek

    r = RegistryIterSeek(Registry())
    r.seek(seek_to)
    assert not list(r)


@given(a=st.integers(min_value=0, max_value=1000),
       b=st.integers(min_value=0, max_value=1000))
def test_registryiterseek_forward_onesegment_unset(a, b):
    assume(len(set([a, b])) == 2)

    from binlog.registry import RegistryIterSeek

    a, b = sorted([a, b])

    r = RegistryIterSeek(Registry([S(a, b)]))

    assert list(r) == list(chain(range(a, b + 1)))


@given(a=st.integers(min_value=0, max_value=1000),
       b=st.integers(min_value=0, max_value=1000),
       seeks=st.sets(st.integers(min_value=0, max_value=1000)))
def test_registryiterseek_forward_onesegment_seek(a, b, seeks):
    assume(len(set([a, b])) == 2)
    a, b = sorted([a, b])

    from binlog.registry import RegistryIterSeek

    r = RegistryIterSeek(Registry([S(a, b)]))

    for s in list(seeks):
        r.seek(s)
        if s < a:
            assert next(r) == a, s
        elif a <= s <= b:
            assert next(r) == s, s
        elif s > b:
            with pytest.raises(StopIteration):
                next(r)
        else:
            assert False, "Non defined %d" % s


@given(a=st.integers(min_value=0, max_value=1000),
       b=st.integers(min_value=0, max_value=1000),
       c=st.integers(min_value=0, max_value=1000),
       d=st.integers(min_value=0, max_value=1000))
def test_registryiterseek_forward_twosegments_unset(a, b, c, d):
    assume(len(set([a, b, c, d])) == 4)
    a, b, c, d = sorted([a, b, c, d])
    assume(b + 1 < c)

    from binlog.registry import RegistryIterSeek

    r = RegistryIterSeek(Registry([S(a, b), S(c, d)]))

    assert list(r) == list(chain(range(a, b + 1), range(c, d + 1)))


@given(a=st.integers(min_value=0, max_value=1000),
       b=st.integers(min_value=0, max_value=1000),
       c=st.integers(min_value=0, max_value=1000),
       d=st.integers(min_value=0, max_value=1000),
       seeks=st.sets(st.integers(min_value=0, max_value=1000)))
def test_registryiterseek_forward_twosegments_seek(a, b, c, d, seeks):
    assume(len(set([a, b, c, d])) == 4)
    a, b, c, d = sorted([a, b, c, d])
    assume(b + 1 < c)

    from binlog.registry import RegistryIterSeek

    r = RegistryIterSeek(Registry([S(a, b), S(c, d)]))

    for s in list(seeks):
        r.seek(s)
        if s < a:
            assert next(r) == a, s
        elif a <= s <= b or c <= s <= d:
            assert next(r) == s, s
        elif b <= s <= c:
            assert next(r) == c, s
        elif s > d:
            with pytest.raises(StopIteration):
                next(r)
        else:
            assert False, "Non defined %d" % s


@given(a=st.integers(min_value=0, max_value=1000),
       b=st.integers(min_value=0, max_value=1000),
       c=st.integers(min_value=0, max_value=1000),
       d=st.integers(min_value=0, max_value=1000),
       e=st.integers(min_value=0, max_value=1000),
       f=st.integers(min_value=0, max_value=1000))
def test_registryiterseek_forward_threesegments_unset(a, b, c, d, e, f):
    assume(len(set([a, b, c, d, e, f])) == 6)
    a, b, c, d, e, f = sorted([a, b, c, d, e, f])
    assume(b + 1 < c and d + 1 < e)

    from binlog.registry import RegistryIterSeek

    r = RegistryIterSeek(Registry([S(a, b), S(c, d), S(e, f)]))

    assert list(r) == list(chain(range(a, b + 1), range(c, d + 1), range(e, f + 1)))


@given(a=st.integers(min_value=0, max_value=1000),
       b=st.integers(min_value=0, max_value=1000),
       c=st.integers(min_value=0, max_value=1000),
       d=st.integers(min_value=0, max_value=1000),
       e=st.integers(min_value=0, max_value=1000),
       f=st.integers(min_value=0, max_value=1000),
       seeks=st.sets(st.integers(min_value=0, max_value=1000)))
def test_registryiterseek_forward_threesegments_seek(a, b, c, d, e, f, seeks):
    assume(len(set([a, b, c, d, e, f])) == 6)
    a, b, c, d, e, f = sorted([a, b, c, d, e, f])
    assume(b + 1 < c and d + 1 < e)

    from binlog.registry import RegistryIterSeek

    r = RegistryIterSeek(Registry([S(a, b), S(c, d), S(e, f)]))

    for s in list(seeks):
        r.seek(s)
        if s < a:
            assert next(r) == a, s
        elif a <= s <= b or c <= s <= d or e <= s <= f:
            assert next(r) == s, s
        elif b <= s <= c:
            assert next(r) == c, s
        elif d <= s <= e:
            assert next(r) == e, s
        elif s > f:
            with pytest.raises(StopIteration):
                next(r)
        else:
            assert False, "Non defined %d" % s

#
# BACKWARD
#
def test_registryiterseek_backward_empty_unset():
    from binlog.registry import RegistryIterSeek

    r = RegistryIterSeek(Registry(), direction=Direction.B)
    assert not list(r)


@given(seek_to=st.integers(min_value=0, max_value=2**64-1))
def test_registryiterseek_backward_empty_seek(seek_to):
    from binlog.registry import RegistryIterSeek

    r = RegistryIterSeek(Registry(), direction=Direction.B)
    r.seek(seek_to)
    assert not list(r)


@given(a=st.integers(min_value=0, max_value=1000),
       b=st.integers(min_value=0, max_value=1000))
def test_registryiterseek_backward_onesegment_unset(a, b):
    assume(len(set([a, b])) == 2)
    a, b = sorted([a, b])

    from binlog.registry import RegistryIterSeek

    r = RegistryIterSeek(Registry([S(a, b)]), direction=Direction.B)

    assert list(r) == list(chain(range(b, a - 1, -1)))


@given(a=st.integers(min_value=0, max_value=1000),
       b=st.integers(min_value=0, max_value=1000),
       seeks=st.sets(st.integers(min_value=0, max_value=1000)))
def test_registryiterseek_backward_onesegment_seek(a, b, seeks):
    assume(len(set([a, b])) == 2)
    a, b = sorted([a, b])

    from binlog.registry import RegistryIterSeek

    r = RegistryIterSeek(Registry([S(a, b)]), direction=Direction.B)

    for s in list(seeks):
        r.seek(s)
        if s < a:
            with pytest.raises(StopIteration):
                next(r)
        elif a <= s <= b:
            assert next(r) == s, s
        elif s > b:
            assert next(r) == b, s
        else:
            assert False, "Non defined %d" % s


@given(a=st.integers(min_value=0, max_value=1000),
       b=st.integers(min_value=0, max_value=1000),
       c=st.integers(min_value=0, max_value=1000),
       d=st.integers(min_value=0, max_value=1000))
def test_registryiterseek_backward_twosegments_unset(a, b, c, d):
    assume(len(set([a, b, c, d])) == 4)
    a, b, c, d = sorted([a, b, c, d])
    assume(b + 1 < c)

    from binlog.registry import RegistryIterSeek

    r = RegistryIterSeek(Registry([S(a, b), S(c, d)]), direction=Direction.B)

    assert list(r) == list(chain(range(d, c - 1, -1),
                                 range(b, a - 1, -1)))


@given(a=st.integers(min_value=0, max_value=1000),
       b=st.integers(min_value=0, max_value=1000),
       c=st.integers(min_value=0, max_value=1000),
       d=st.integers(min_value=0, max_value=1000),
       seeks=st.sets(st.integers(min_value=0, max_value=1000)))
def test_registryiterseek_backward_twosegments_seek(a, b, c, d, seeks):
    assume(len(set([a, b, c, d])) == 4)
    a, b, c, d = sorted([a, b, c, d])
    assume(b + 1 < c)

    from binlog.registry import RegistryIterSeek

    r = RegistryIterSeek(Registry([S(a, b), S(c, d)]), direction=Direction.B)

    for s in list(seeks):
        r.seek(s)
        if s < a:
            with pytest.raises(StopIteration):
                next(r)
        elif a <= s <= b or c <= s <= d:
            assert next(r) == s, s
        elif b <= s <= c:
            assert next(r) == b, s
        elif s > d:
            assert next(r) == d, s
        else:
            assert False, "Non defined %d" % s


@given(a=st.integers(min_value=0, max_value=1000),
       b=st.integers(min_value=0, max_value=1000),
       c=st.integers(min_value=0, max_value=1000),
       d=st.integers(min_value=0, max_value=1000),
       e=st.integers(min_value=0, max_value=1000),
       f=st.integers(min_value=0, max_value=1000))
def test_registryiterseek_backward_threesegments_unset(a, b, c, d, e, f):
    assume(len(set([a, b, c, d, e, f])) == 6)
    a, b, c, d, e, f = sorted([a, b, c, d, e, f])
    assume(b + 1 < c and d + 1 < e)

    from binlog.registry import RegistryIterSeek

    r = RegistryIterSeek(Registry([S(a, b), S(c, d), S(e, f)]),
                         direction=Direction.B)

    assert list(r) == list(chain(range(f, e - 1, -1),
                                 range(d, c - 1, -1),
                                 range(b, a - 1, -1)))


@given(a=st.integers(min_value=0, max_value=1000),
       b=st.integers(min_value=0, max_value=1000),
       c=st.integers(min_value=0, max_value=1000),
       d=st.integers(min_value=0, max_value=1000),
       e=st.integers(min_value=0, max_value=1000),
       f=st.integers(min_value=0, max_value=1000),
       seeks=st.sets(st.integers(min_value=0, max_value=1000)))
def test_registryiterseek_backward_threesegments_seek(a, b, c, d, e, f, seeks):
    assume(len(set([a, b, c, d, e, f])) == 6)
    a, b, c, d, e, f = sorted([a, b, c, d, e, f])
    assume(b + 1 < c and d + 1 < e)

    from binlog.registry import RegistryIterSeek

    r = RegistryIterSeek(Registry([S(a, b), S(c, d), S(e, f)]),
                         direction=Direction.B)

    for s in list(seeks):
        r.seek(s)
        if s < a:
            with pytest.raises(StopIteration):
                next(r)
        elif a <= s <= b or c <= s <= d or e <= s <= f:
            assert next(r) == s, s
        elif b <= s <= c:
            assert next(r) == b, s
        elif d <= s <= e:
            assert next(r) == d, s
        elif s > f:
            assert next(r) == f, s
        else:
            assert False, "Non defined %d" % s
