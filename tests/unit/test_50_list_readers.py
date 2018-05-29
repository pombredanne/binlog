from string import ascii_lowercase
from tempfile import TemporaryDirectory

from hypothesis import given, assume
from hypothesis import strategies as st
import pytest

from binlog.model import Model
from binlog.connection import RESERVED_READER_NAMES


@given(readers=st.sets(st.text(min_size=1,
                               max_size=511,
                               alphabet=ascii_lowercase)))
def test_list_readers(readers):
    assume(len(readers & RESERVED_READER_NAMES) == 0)

    with TemporaryDirectory() as tmpdir:
        with Model.open(tmpdir) as db:
            for name in readers:
                db.register_reader(name)

            assert set(db.list_readers()) == readers


def test_list_readers_ignore_hints(tmpdir):
    with Model.open(tmpdir) as db:

        db.register_reader("myreader")

        with db.reader("myreader") as r1:
            # This will create the `hints` db.
            r1._save_hint(0)

        assert db.list_readers() == ["myreader"]
