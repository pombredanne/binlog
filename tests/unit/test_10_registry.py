import random

from hypothesis import given
from hypothesis import strategies as st
import pytest


def test_registry_exists():
    try:
        from binlog.registry import Registry
    except ImportError as exc:
        assert False, exc


def test_registry_initialization():
    from binlog.registry import Registry

    r = Registry()

    assert r.initial == None
    assert r.acked == []


@given(data=st.integers())
def test_registry_add_sets_initial(data):
    from binlog.registry import Registry

    r = Registry()

    r.add(data)
    assert r.initial == data


@given(data=st.integers())
def test_registry_add_starts_range(data):
    from binlog.registry import Registry

    r = Registry()

    assert r.add(data)
    assert r.acked == [(data, data)]


def test_registry_add_only_accept_integers():
    from binlog.registry import Registry

    r = Registry()

    with pytest.raises(ValueError):
        r.add(None)


@given(data=st.integers(min_value=10))
def test_registry_add_non_consecutive(data):
    from binlog.registry import Registry

    r = Registry()

    r.add(0)
    assert (0, 0) in r.acked

    r.add(data)
    assert (data, data) in r.acked


@given(data=st.integers(min_value=0))
def test_registry_add_consecutive_right_side(data):
    from binlog.registry import Registry

    r = Registry()

    r.add(data)
    r.add(data + 1)

    assert (data, data + 1) in r.acked


@given(data=st.integers(min_value=1))
def test_registry_add_consecutive_left_side(data):
    from binlog.registry import Registry

    r = Registry()

    r.add(data)
    r.add(data - 1)

    assert (data - 1, data) in r.acked


@given(data=st.integers(min_value=1))
def test_registry_add_consecutive_left_and_right(data):
    from binlog.registry import Registry

    r = Registry()

    r.add(data - 1)
    r.add(data + 1)
    r.add(data)

    assert (data - 1, data + 1) in r.acked


def test_registry_double_add():
    from binlog.registry import Registry

    r = Registry()

    assert r.add(0)
    assert not r.add(0)


@given(data=st.integers(min_value=-100, max_value=100))
def test_registry_add_randomized_range(data):
    from binlog.registry import Registry

    r = Registry()

    l = list(range(data, data + 101)) * random.randint(1, 3)
    random.shuffle(l)

    for i in l:
        r.add(i)

    assert (data, data + 100) in r.acked


@given(data=st.lists(st.integers(min_value=-100, max_value=100)))
def test_registry_is_always_sorted(data):
    from binlog.registry import Registry

    r = Registry()

    for i in data:
        r.add(i)

    assert r.acked == sorted(r.acked)


@given(data=st.lists(st.integers(min_value=-100, max_value=100)),
       point=st.integers(min_value=-100, max_value=100))
def test_registry_contains(data, point):
    from binlog.registry import Registry

    r = Registry()

    for i in data:
        r.add(i)

    assert (point in r) == (point in data)
