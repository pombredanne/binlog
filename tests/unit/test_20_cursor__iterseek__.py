from hypothesis import given, assume, example
from hypothesis import strategies as st
import pytest

from binlog.abstract import Direction


#
# Both directions
#
@pytest.mark.parametrize("direction", Direction) 
def test_cursorproxy_dup_empty(testenv, direction):
    with testenv(dupsort=True) as database:
        with database(direction=direction) as cursor:
            with pytest.raises(ValueError):
                cursor.dupkey = 0


@pytest.mark.parametrize("direction", Direction) 
def test_cursorproxy_dup_iter_noset_dupkey(testenv, direction):
    with testenv(dupsort=True) as database:
        with database(direction=direction) as cursor:
            with pytest.raises(RuntimeError):
                assert not list(cursor)


@pytest.mark.parametrize("direction", Direction) 
def test_cursorproxy_nodup_empty_noseek(testenv, direction):
    with testenv(dupsort=False) as database:
        with database(direction=direction) as cursor:
            assert not list(cursor)


@pytest.mark.parametrize("direction", Direction) 
@given(seek_to=st.integers(min_value=0, max_value=2**64-1))
def test_cursorproxy_nodup_empty_seek(testenv, direction, seek_to):
    with testenv(dupsort=False) as database:
        with database(direction=direction) as cursor:
            cursor.seek(seek_to)
            assert not list(cursor)


#
# FORWARD
#
@pytest.mark.parametrize("dupsort", [True, False])
@given(values=st.sets(st.integers(min_value=0, max_value=2**64-1), min_size=1))
def test_cursorproxy_forward_nonempty_noseek(testenv, dupsort, values):
    values = list(sorted(values))
    if dupsort:
        expected = [(1, k) for k in values]
    else:
        expected = [(k, k) for k in values]

    with testenv(dupsort=dupsort) as database:
        with database() as cursor:
            for k, v in expected:
                if dupsort:
                    cursor.put(k - 1, v)
                    cursor.put(k + 1, v)
                cursor.put(k, v)

        with database() as cursor:
            if dupsort:
                cursor.dupkey = 1
                assert list(cursor) == [v for _, v in expected]
            else:
                assert list(cursor) == expected


@pytest.mark.parametrize("dupsort", [True, False])
@given(values=st.sets(st.integers(min_value=0, max_value=2**64-1), min_size=1),
       seeks=st.sets(st.integers(min_value=0, max_value=2**64-1), min_size=1))
@example(values=[0], seeks=[0, 256])
def test_cursorproxy_forward_nonempty_seek(testenv, dupsort, values, seeks):
    values = list(sorted(values))
    if dupsort:
        expected = [(1, k) for k in values]
    else:
        expected = [(k, k) for k in values]

    with testenv(dupsort=dupsort) as database:
        with database() as cursor:
            for k, v in expected:
                if dupsort:
                    cursor.put(k - 1, v)
                    cursor.put(k + 1, v)
                cursor.put(k, v)

        with database() as cursor:
            if dupsort:
                cursor.dupkey = 1

            for s in list(seeks):
                cursor.seek(s)
                closer = min([x for x in values if x >= s], default=None)
                if closer is not None:
                    if dupsort:
                        assert list(cursor) == [v for _, v in expected[values.index(closer):]]
                    else:
                        assert list(cursor) == expected[values.index(closer):]
                else:
                    assert list(cursor) == []


#
# BACKWARD
#
@pytest.mark.parametrize("dupsort", [True, False])
@given(values=st.sets(st.integers(min_value=0, max_value=2**64-1), min_size=1))
def test_cursorproxy_backward_nonempty_noseek(testenv, dupsort, values):
    values = list(sorted(values))
    if dupsort:
        expected = [(1, k) for k in values]
    else:
        expected = [(k, k) for k in values]

    with testenv(dupsort=dupsort) as database:
        with database(direction=Direction.B) as cursor:
            for k, v in expected:
                if dupsort:
                    cursor.put(k - 1, v)
                    cursor.put(k + 1, v)
                cursor.put(k, v)

        with database(direction=Direction.B) as cursor:
            if dupsort:
                cursor.dupkey = 1
                assert list(cursor) == [v for _, v in reversed(expected)]
            else:
                assert list(cursor) == list(reversed(expected))


@pytest.mark.parametrize("dupsort", [True, False])
@given(values=st.sets(st.integers(min_value=0, max_value=2**64-1), min_size=1),
       seeks=st.sets(st.integers(min_value=0, max_value=2**64-1), min_size=1))
def test_cursorproxy_backward_nonempty_seek(testenv, dupsort, values, seeks):
    values = list(sorted(values))
    if dupsort:
        expected = [(1, k) for k in values]
    else:
        expected = [(k, k) for k in values]

    with testenv(dupsort=dupsort) as database:
        with database(direction=Direction.B) as cursor:
            for k, v in expected:
                if dupsort:
                    cursor.put(k - 1, v)
                    cursor.put(k + 1, v)
                cursor.put(k, v)

        with database(direction=Direction.B) as cursor:
            if dupsort:
                cursor.dupkey = 1

            for s in list(seeks):
                cursor.seek(s)
                closer = max([x for x in values if x <= s], default=None)
                if closer is not None:
                    if dupsort:
                        assert list(cursor) == [v for _, v in expected[values.index(closer)::-1]]
                    else:
                        assert list(cursor) == expected[values.index(closer)::-1]
                else:
                    assert list(cursor) == []
