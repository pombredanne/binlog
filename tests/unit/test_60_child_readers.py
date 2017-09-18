import pytest

from binlog.model import Model


def test_child_reader(tmpdir):
    with Model.open(tmpdir) as db:
        db.register_reader('parent.child')

        # SHOULD NOT RAISE
        with db.reader('parent'):
            pass

        with db.reader('parent.child'):
            pass


def test_child_reader_acks_dont_affect_parents(tmpdir):
    with Model.open(tmpdir) as db:

        db.create(idx=0)

        db.register_reader('parent.child')

        with db.reader('parent.child') as reader:
            reader.ack(reader[0])

        with db.reader('parent.child') as reader:
            assert reader.is_acked(reader[0])

        with db.reader('parent') as reader:
            assert not reader.is_acked(reader[0])


def test_child_reader_recursive_ack(tmpdir):
    with Model.open(tmpdir) as db:

        db.create(idx=0)

        db.register_reader('parent.child.grandchild')

        with db.reader('parent.child.grandchild') as reader:
            reader.recursive_ack(reader[0])

        with db.reader('parent.child.grandchild') as reader:
            assert reader.is_acked(reader[0])

        with db.reader('parent.child') as reader:
            assert reader.is_acked(reader[0])

        with db.reader('parent') as reader:
            assert reader.is_acked(reader[0])


def test_multiple_child_reader_recursive_ack(tmpdir):
    with Model.open(tmpdir) as db:

        entries = [Model(idx=i) for i in range(10)]
        db.bulk_create(entries)

        db.register_reader('parent.child.grandchild1')
        db.register_reader('parent.child.grandchild2')

        with db.reader('parent.child.grandchild1') as reader:
            for i in range(5, 10):
                reader.recursive_ack(reader[i])

        with db.reader('parent.child.grandchild1') as reader:
            assert list(reader) == entries[:5]

        with db.reader('parent.child.grandchild2') as reader:
            for i in range(5):
                reader.recursive_ack(reader[i])

        with db.reader('parent.child.grandchild2') as reader:
            assert list(reader) == entries[5:]

        with db.reader('parent.child') as reader:
            assert not list(reader)

        with db.reader('parent') as reader:
            assert not list(reader)


def test_multiple_child_reader_recursive_ack_results(tmpdir):
    with Model.open(tmpdir) as db:

        entries = [Model(idx=i) for i in range(10)]
        db.bulk_create(entries)

        db.register_reader('parent.child.grandchild1')
        db.register_reader('parent.child.grandchild2')

        with db.reader('parent.child.grandchild1') as reader:
            for i in range(5, 10):
                assert reader.recursive_ack(reader[i])

        with db.reader('parent.child.grandchild1') as reader:
            for i in range(5, 10):
                assert not reader.recursive_ack(reader[i])

        with db.reader('parent.child.grandchild2') as reader:
            for i in range(10):
                assert reader.recursive_ack(reader[i])

        with db.reader('parent.child') as reader:
            for i in range(10):
                assert not reader.recursive_ack(reader[i])

        with db.reader('parent') as reader:
            for i in range(10):
                assert not reader.recursive_ack(reader[i])


def test_clone_reader_inherits_progress(tmpdir):
    with Model.open(tmpdir) as db:

        entries = [Model(idx=i) for i in range(10)]
        db.bulk_create(entries)

        db.register_reader('reader1')

        with db.reader('reader1') as reader:
            for i in range(5):
                assert reader.ack(i)

        with db.reader('reader1') as reader:
            for a, b in zip(reader, range(5, 10)):
                assert a["idx"] == b

        db.clone_reader('reader1', 'reader2')

        with db.reader('reader2') as reader:
            for a, b in zip(reader, range(5, 10)):
                assert a["idx"] == b

        with db.reader('reader1') as reader:
            for i in reader:
                assert reader.ack(i)

        with db.reader('reader2') as reader:
            for a, b in zip(reader, range(5, 10)):
                assert a["idx"] == b
