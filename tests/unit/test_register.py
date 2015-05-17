import pytest
from pytest import dict_of
from random import shuffle, randint

from binlog import register
from binlog.binlog import Record


#
# Register
#
def test_Register_exists():
    """The Register exists."""
    assert hasattr(register, 'Register')

#
# Register().reg
#
def test_Register_None_reg_makes_empty_reg():
    """
    If `reg` argument is not passed into Register, then the `reg`
    attribute must be an empty dictionary.
    """

    r = register.Register()

    assert r.reg == {}


def test_Register_reg_is_a_copy():
    """The attribute Register.reg must be a copy of the `reg` parameter."""

    original = {1: [(1, 20), (30, 30)],
                2: [(2, 2)]}
    r = register.Register(original)

    assert r.reg == original
    assert r.reg is not original


#
# Register().add
#
def test_Register_add():
    """The Register has the add method."""
    assert hasattr(register.Register, 'add')


@pytest.mark.randomize(data=(int, int, str), ncalls=100)
def test_Register_add_on_empty(data):
    """
    When the add method is called on an empty Register, the `reg`
    attribute must have the same data as the record.
    """
    original = Record(*data)
    r = register.Register()
    r.add(original)

    assert original.liidx in r.reg
    assert [(original.clidx, original.clidx)] == r.reg[original.liidx]


@pytest.mark.randomize(data=(int, int, str), min_num=2, ncalls=100)
def test_Register_add_but_different_liidx(data):
    """
    When the add method is called on a non empty Register. Then, the
    `reg` attribute must have the new `liidx` if the previous Record
    added have a different `liidx`.
    """

    r = register.Register()
    r.add(Record(liidx=1, clidx=1, value='value'))

    original = Record(*data)
    r.add(original)

    assert original.liidx in r.reg
    assert [(original.clidx, original.clidx)] == r.reg[original.liidx]


@pytest.mark.randomize(clidx=int, min_num=10, ncalls=100)
def test_Register_add_same_liidx_non_consecutive_clidx(clidx):
    """
    When the add method is called on a non empty Register. Then, the key
    liidx on the `reg` attribute must have the new clidx appended in a
    tuple with this shape (clidx, clidx).
    """

    r = register.Register()
    r.add(Record(liidx=1, clidx=2, value='first'))
    r.add(Record(liidx=1, clidx=clidx, value='second'))

    assert (2, 2) in r.reg[1]
    assert (clidx, clidx) in r.reg[1]


@pytest.mark.randomize(clidx=int, ncalls=100)
def test_Register_add_same_liidx_and_consecutive_clidx_upperbound(clidx):
    """
    If in the llidx key of the `reg` attribute exists a tuple which
    right value is the previous value of this clidx. Then, this tuple
    must be replaced with one in which the right value must be this
    clidx. Ex::

    Register.reg[1] = [(1, 4)]

    liidx = 1
    clidx = 5
    Register.add(Record(liidx, clidx, 'something'))
    Register.reg[1] = [(1, 5)]

    """

    r = register.Register()
    r.add(Record(liidx=1, clidx=clidx, value='first'))
    r.add(Record(liidx=1, clidx=clidx+1, value='second'))

    assert [(clidx, clidx+1)] == r.reg[1]


@pytest.mark.randomize(clidx=int, ncalls=100)
def test_Register_add_same_liidx_and_consecutive_clidx_lowerbound(clidx):
    """
    If in the llidx key of the `reg` attribute exists a tuple which
    left value is the next value of this clidx. Then, this tuple
    must be replaced with one in which the left value must be this
    clidx. Ex::

    Register.reg[1] = [(4, 9)]

    liidx = 1
    clidx = 3
    Register.add(Record(liidx, clidx, 'something'))
    Register.reg[1] = [(3, 9)]

    """

    r = register.Register()
    r.add(Record(liidx=1, clidx=clidx, value='first'))
    r.add(Record(liidx=1, clidx=clidx-1, value='second'))

    assert [(clidx-1, clidx)] == r.reg[1]


@pytest.mark.randomize(clidx=int, ncalls=100)
def test_Register_add_same_liidx_and_consecutive_clidx_lowerbound_and_upperbound(clidx):
    """

    If in the llidx key of the `reg` attribute exists a tuple which
    right value is the previous value of this clidx, and also exists
    other tuple which left value is the next value of the clidx. Then,
    those tuples must be merged in one in which left value must belong
    to the first tuple and the right value to the second one. Ex::

    Register.reg[1] = [(1, 3), (5, 9)]

    liidx = 1
    clidx = 4
    Register.add(Record(liidx, clidx, 'something'))
    Register.reg[1] = [(1, 9)]

    """

    r = register.Register()
    r.add(Record(liidx=1, clidx=clidx-1, value='first'))
    r.add(Record(liidx=1, clidx=clidx+1, value='second'))
    r.add(Record(liidx=1, clidx=clidx, value='second'))

    assert [(clidx-1, clidx+1)] == r.reg[1]


@pytest.mark.randomize(clidx=int, min_num=-50, max_num=50, ncalls=100)
def test_Register_add_same_liidx_inside_existing_range(clidx):
    """
    If the clidx was added before, just ignore it.
    """

    r = register.Register()
    for i in range(-50, 51):
        r.add(Record(liidx=1, clidx=i, value='first'))

    r.add(Record(liidx=1, clidx=clidx, value='second'))

    assert [(-50, 50)] == r.reg[1]


@pytest.mark.randomize(clidx=int, min_num=-100, max_num=100, ncalls=100)
def test_Register_add_randomized_range(clidx):
    """
    """
    r = register.Register()

    l = list(range(clidx, clidx+101)) * randint(1, 3)
    shuffle(l)
    for i in l:
        r.add(Record(liidx=1, clidx=i, value='data'))

    assert [(clidx, clidx+100)] == r.reg[1]
