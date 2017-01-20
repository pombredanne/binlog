import pytest

from binlog.model import Model


def test_event_is_acked(tmpdir):
    with Model.open(tmpdir) as db:
        entry = db.create(test='data')

        db.register_reader('myreader')
        with db.reader('myreader') as reader:
            e = reader[0]
            assert e.pk not in reader.registry
            reader.ack(reader[0])
            assert e.pk in reader.registry


def test_event_is_acked_after_commit(tmpdir):
    with Model.open(tmpdir) as db:
        entry = db.create(test='data')

        db.register_reader('myreader')
        with db.reader('myreader') as reader:
            e = reader[0]
            reader.ack(reader[0])

        # Reader commits registry on exit

        with db.reader('myreader') as reader:
            assert e.pk in reader.registry


def test_ack_on_unsaved_event(tmpdir):
    with Model.open(tmpdir) as db:
        db.register_reader('myreader')
        with db.reader('myreader') as reader:
            with pytest.raises(ValueError):
                reader.ack(Model(test='data'))


def test_ack_on_anonymous_reader(tmpdir):
    with Model.open(tmpdir) as db:
        entry = db.create(test='data')

        with db.reader() as reader:
            with pytest.raises(RuntimeError):
                reader.ack(reader[0])
