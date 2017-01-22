import pytest

from binlog.model import Model


@pytest.mark.wip
def test_purge_without_readers(tmpdir):

    with Model.open(tmpdir) as db:
        db.create(test='data')

        assert db.purge() == 0

