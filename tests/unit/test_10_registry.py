import random
from collections import deque

from hypothesis import given, example
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
    assert r.acked == deque()


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
    assert r.acked == deque([(data, data)])


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

    assert list(r.acked) == sorted(r.acked)  # sorted always returns list


@given(data=st.lists(st.integers(min_value=-100, max_value=100)),
       point=st.integers(min_value=-100, max_value=100))
def test_registry_contains(data, point):
    from binlog.registry import Registry

    r = Registry()

    for i in data:
        r.add(i)

    assert (point in r) == (point in data)


@given(data_a=st.sets(st.integers(min_value=0, max_value=20)),
       data_b=st.sets(st.integers(min_value=0, max_value=20)))
@example(data_a={0}, data_b={0})
def test_registries_support_union(data_a, data_b):
    from binlog.registry import Registry

    registry_a = Registry()
    registry_b = Registry()

    for p in data_a:
        registry_a.add(p)

    for p in data_b:
        registry_b.add(p)

    registry_x = registry_a | registry_b

    ack_points = data_a | data_b
    non_ack_points = set(range(0, 20)) - ack_points

    for p in ack_points:
        assert p in registry_x

    for p in non_ack_points:
        assert p not in registry_x


def test_registries_support_union__bigger_segment():
    from binlog.registry import Registry, S

    registry_a = Registry(acked=deque([S(0, 10)]))
    registry_b = Registry(acked=deque([S(5, 8)]))

    registry_x1 = registry_a | registry_b
    registry_x2 = registry_b | registry_a

    assert registry_x1.acked == registry_x2.acked == deque([S(0, 10)])


def test_registries_support_union__overlap_segments():
    from binlog.registry import Registry, S

    registry_a = Registry(acked=deque([S(0, 10)]))
    registry_b = Registry(acked=deque([S(5, 20)]))

    registry_x1 = registry_a | registry_b
    registry_x2 = registry_b | registry_a

    assert registry_x1.acked == registry_x2.acked == deque([S(0, 20)])


def test_registries_support_union__start_and_end_are_consecutive():
    from binlog.registry import Registry, S

    registry_a = Registry(acked=deque([S(0, 10)]))
    registry_b = Registry(acked=deque([S(11, 20)]))

    registry_x1 = registry_a | registry_b
    registry_x2 = registry_b | registry_a

    assert registry_x1.acked == registry_x2.acked == deque([S(0, 20)])


@given(data_a=st.sets(st.integers(min_value=0, max_value=20)),
       data_b=st.sets(st.integers(min_value=0, max_value=20)))
@example(data_a={11, 12, 13, 14}, data_b={12, 14})
@example(data_a={4, 5, 6}, data_b={4, 6})
def test_registries_support_intersection(data_a, data_b):
    from binlog.registry import Registry

    registry_a = Registry()
    registry_b = Registry()

    for p in data_a:
        registry_a.add(p)

    for p in data_b:
        registry_b.add(p)

    registry_x = registry_a & registry_b

    ack_points = data_a & data_b
    non_ack_points = set(range(0, 20)) - ack_points

    for p in ack_points:
        assert p in registry_x

    for p in non_ack_points:
        assert p not in registry_x


def test_registries_support_intersection__bigger_segment():
    from binlog.registry import Registry, S

    registry_a = Registry(acked=deque([S(0, 10)]))
    registry_b = Registry(acked=deque([S(5, 8)]))

    registry_x1 = registry_a & registry_b
    registry_x2 = registry_b & registry_a

    assert registry_x1.acked == registry_x2.acked == deque([S(5, 8)])


def test_registries_support_intersection__overlap_segments():
    from binlog.registry import Registry, S

    registry_a = Registry(acked=deque([S(0, 10)]))
    registry_b = Registry(acked=deque([S(5, 20)]))

    registry_x1 = registry_a & registry_b
    registry_x2 = registry_b & registry_a

    assert registry_x1.acked == registry_x2.acked == deque([S(5, 10)])


def test_registries_support_intersection__start_and_end_are_consecutive():
    from binlog.registry import Registry, S

    registry_a = Registry(acked=deque([S(0, 10)]))
    registry_b = Registry(acked=deque([S(11, 20)]))

    registry_x1 = registry_a & registry_b
    registry_x2 = registry_b & registry_a

    assert registry_x1.acked == registry_x2.acked == deque([])
