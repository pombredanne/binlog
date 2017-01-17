import pytest
from binlog.model import Model
from binlog.index import Index


def test_model_with_index():
    """
    When a subclass of Index is added to a model as an atribute the
    class constructor must add a reference of the index in the _indexes
    dictionary.

    """

    class CustomModel(Model):
        myindex = Index()

    c = CustomModel()

    assert 'myindex' in c._indexes

    with pytest.raises(AttributeError):
        c.myindex
