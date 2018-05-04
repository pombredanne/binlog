from datetime import datetime

from hypothesis import given, example
from hypothesis import strategies as st
import pytest

from binlog.index import TextIndex, NumericIndex, DatetimeIndex
from binlog.serializer import TextSerializer
from binlog.util import cmp


@pytest.mark.parametrize(
    "index, key_serializer",
    [(TextIndex, TextSerializer)])
def test_index_serializers(index, key_serializer):
    assert index.K is key_serializer


def _test_index_is_sortable(serializer, python_value1, python_value2):
    db_value1 = serializer.db_value(python_value1)
    db_value2 = serializer.db_value(python_value2)

    cmp_python = cmp(python_value1, python_value2)
    cmp_db = cmp(db_value1, db_value2)

    return cmp_python == cmp_db


@given(python_value1=st.text(min_size=0, max_size=511),
       python_value2=st.text(min_size=0, max_size=511))
def test_TextIndex_is_sortable(python_value1, python_value2):
    assert _test_index_is_sortable(TextIndex.K,
                                   python_value1,
                                   python_value2)


@given(python_value1=st.integers(min_value=0, max_value=2**64-1),
       python_value2=st.integers(min_value=0, max_value=2**64-1))
@example(python_value1=1, python_value2=256)
def test_NumericIndex_is_sortable(python_value1, python_value2):
    assert _test_index_is_sortable(NumericIndex.K,
                                   python_value1,
                                   python_value2)


@given(python_value1=st.datetimes(min_value=datetime.fromtimestamp(0)),
       python_value2=st.datetimes(min_value=datetime.fromtimestamp(0)))
def test_DatetimeIndex_is_sortable(python_value1, python_value2):
    assert _test_index_is_sortable(DatetimeIndex.K,
                                   python_value1,
                                   python_value2)
