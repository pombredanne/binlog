from hypothesis import given
from hypothesis.extra import datetime
from string import ascii_letters
import hypothesis.strategies as st
import pytest

from binlog.serializer import NumericSerializer
from binlog.serializer import ObjectSerializer
from binlog.serializer import NullListSerializer
from binlog.serializer import TextSerializer
from binlog.serializer import DatetimeSerializer


@pytest.mark.parametrize(
    "serializer,strategy",
    [(NumericSerializer, st.integers(min_value=0, max_value=2**64-1)),
     (TextSerializer, st.text(min_size=0, max_size=511)),
     (ObjectSerializer, st.dictionaries(st.text(), st.text())),
     (NullListSerializer, st.text(min_size=1,
                                  alphabet=ascii_letters + '.')),
     (DatetimeSerializer, datetime.datetimes(timezones=[],
                                             min_year=1970))])
@given(st.data())
def test_serializers_conversion(serializer, strategy, data): 
    python_value = expected = data.draw(strategy) 
    current = serializer.python_value(
        memoryview(serializer.db_value(python_value)))

    assert current == expected


def test_nulllistserializer_invalid_values():
    with pytest.raises(ValueError):
        NullListSerializer.db_value('')

    with pytest.raises(ValueError):
        NullListSerializer.db_value('test\0')

    with pytest.raises(ValueError):
        NullListSerializer.db_value('ñoño')
