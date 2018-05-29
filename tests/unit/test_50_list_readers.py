from string import ascii_lowercase
from tempfile import TemporaryDirectory

from hypothesis import given
from hypothesis import strategies as st
import pytest

from binlog.model import Model


@given(readers=st.sets(st.text(min_size=1,
                               max_size=511,
                               alphabet=ascii_lowercase)))
def test_list_readers(readers):
    with TemporaryDirectory() as tmpdir:
        with Model.open(tmpdir) as db:
            for name in readers:
                db.register_reader(name)

            # 'hints' should be ignored by list_readers()
            db.register_reader('hints')

            assert set(db.list_readers()) == readers
