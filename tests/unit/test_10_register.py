import random

from hypothesis import given
from hypothesis import strategies as st
import pytest


def test_register_exists():
    try:
        from binlog.register import Register
    except ImportError as exc:
        assert False, exc


def test_register_initialization():
    from binlog.register import Register

    r = Register()

    assert r.initial == None
    assert r.acked == []


@given(data=st.integers())
def test_register_add_sets_initial(data):
    from binlog.register import Register

    r = Register()

    r.add(data)
    assert r.initial == data


@given(data=st.integers())
def test_register_add_starts_range(data):
    from binlog.register import Register

    r = Register()

    assert r.add(data)
    assert r.acked == [(data, data)]


def test_register_add_only_accept_integers():
    from binlog.register import Register

    r = Register()

    with pytest.raises(ValueError):
        r.add(None)


@given(data=st.integers(min_value=10))
def test_register_add_non_consecutive(data):
    from binlog.register import Register

    r = Register()

    r.add(0)
    assert (0, 0) in r.acked

    r.add(data)
    assert (data, data) in r.acked


@given(data=st.integers(min_value=0))
def test_register_add_consecutive_right_side(data):
    from binlog.register import Register

    r = Register()

    r.add(data)
    r.add(data + 1)

    assert (data, data + 1) in r.acked


@given(data=st.integers(min_value=1))
def test_register_add_consecutive_left_side(data):
    from binlog.register import Register

    r = Register()

    r.add(data)
    r.add(data - 1)

    assert (data - 1, data) in r.acked


@given(data=st.integers(min_value=1))
def test_register_add_consecutive_left_and_right(data):
    from binlog.register import Register

    r = Register()

    r.add(data - 1)
    r.add(data + 1)
    r.add(data)

    assert (data - 1, data + 1) in r.acked


def test_register_double_add():
    from binlog.register import Register

    r = Register()

    assert r.add(0)
    assert not r.add(0)


@given(data=st.integers(min_value=-100, max_value=100))
def test_register_add_randomized_range(data):
    from binlog.register import Register

    r = Register()

    l = list(range(data, data + 101)) * random.randint(1, 3)
    random.shuffle(l)

    for i in l:
        r.add(i)

    assert (data, data + 100) in r.acked


@given(data=st.lists(st.integers(min_value=-100, max_value=100)))
def test_register_is_always_sorted(data):
    from binlog.register import Register

    r = Register()

    for i in data:
        r.add(i)

    assert r.acked == sorted(r.acked)


@given(data=st.lists(st.integers(min_value=-100, max_value=100)),
       point=st.integers(min_value=-100, max_value=100))
def test_register_contains(data, point):
    from binlog.register import Register

    r = Register()

    for i in data:
        r.add(i)

    assert (point in r) == (point in data)
