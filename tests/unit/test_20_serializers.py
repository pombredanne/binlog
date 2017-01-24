from hypothesis import given
from hypothesis.strategies import integers, text, data, dictionaries, lists
import pytest
from string import ascii_letters

from binlog.serializer import NumericSerializer
from binlog.serializer import ObjectSerializer
from binlog.serializer import StringListSerializer
from binlog.serializer import TextSerializer


@pytest.mark.parametrize(
    "serializer,strategy",
    [(NumericSerializer, integers(min_value=0, max_value=2**64-1)),
     (TextSerializer, text(min_size=0, max_size=511)),
     (ObjectSerializer, dictionaries(text(), text())),
     (StringListSerializer, lists(text(min_size=1,
                                       alphabet=ascii_letters),
                                  min_size=1)) ])
@given(data())
def test_serializers_conversion(serializer, strategy, data): 
    python_value = expected = data.draw(strategy) 
    current = serializer.python_value(memoryview(serializer.db_value(python_value)))

    assert current == expected


def test_stringlistserializer_invalid_values():
    with pytest.raises(ValueError):
        StringListSerializer.db_value([])

    with pytest.raises(ValueError):
        StringListSerializer.db_value([''])

    with pytest.raises(ValueError):
        StringListSerializer.db_value(['test\0'])

    with pytest.raises(ValueError):
        StringListSerializer.db_value(['ñoño'])
