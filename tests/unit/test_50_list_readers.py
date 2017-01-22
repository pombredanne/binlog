from string import ascii_letters
from tempfile import TemporaryDirectory

from hypothesis import given
from hypothesis import strategies as st
import pytest

from binlog.model import Model


@given(readers=st.sets(st.text(min_size=1,
                               max_size=511,
                               alphabet=ascii_letters)))
def test_list_readers(readers):
    with TemporaryDirectory() as tmpdir:
        with Model.open(tmpdir) as db:
            for name in readers:
                db.register_reader(name)

            assert set(db.list_readers()) == readers
