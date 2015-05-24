from tempfile import mktemp
import shutil

import pytest

from binlog import queue, writer, reader


#
# Queue
#
def test_Queue_exists():
    """The Queue class exists."""
    assert hasattr(queue, 'Queue')

#
# Queue().get_nowait
#
def test_Queue_get_nowait():
    """The Queue class has the get_nowait method."""
    assert hasattr(queue.Queue, 'get_nowait')

#
# Queue().get_nowait
#
def test_Queue_get_nowait_on_writed_queue():
    """If there is data in the queue get_nowait must return it."""
    try:
        tmpdir = mktemp()

        # Write 10 entries
        w = writer.Writer(tmpdir)
        for i in range(10):
            w.append(i)
        w.set_current_log().sync()

        q = queue.Queue(tmpdir)
        for i in range(10):
            assert q.get_nowait().value == i
        assert q.get_nowait() is None
    except:
        raise
    finally:
        shutil.rmtree(tmpdir)


def test_Queue_get_nowait_on_empty_queue():
    """Queue.get_nowait on an empty queue must return None **always**."""
    tmpdir = mktemp()
    q = queue.Queue(tmpdir)
    assert q.get_nowait() is None


#
# Queue().get
#
def test_Queue_get():
    """The Queue class has the get method."""
    assert hasattr(queue.Queue, 'get')


def test_Queue_get_waits_until_data():
    """The Queue.get method must wait until there is data in the queue."""
    from time import sleep
    from threading import Thread

    try:
        tmpdir = mktemp()

        def insert_thread():
            w = writer.Writer(tmpdir)
            for i in range(10):
                w.append(i)
                sleep(0.1)
                w.set_current_log().sync()

        t = Thread(target=insert_thread)
        t.daemon = True
        t.start()

        q = queue.Queue(tmpdir)
        for i in range(10):
            assert q.get().value == i

        t.join()
    except:
        raise
    finally:
        shutil.rmtree(tmpdir)

#
# Queue().put
#
def test_Queue_put():
    """The Queue class has the put method."""
    assert hasattr(queue.Queue, 'put')


def test_Queue_put_on_new_queue():
    try:
        tmpdir = mktemp()
        q = queue.Queue(tmpdir)
        for i in range(10):
            q.put(i)

        r = reader.Reader(tmpdir)
        for i in range(10):
            assert r.next_record().value == i

        assert r.next_record() is None

    except:
        raise
    finally:
        shutil.rmtree(tmpdir)


def test_Queue_put_and_get():
    try:
        tmpdir = mktemp()
        q = queue.Queue(tmpdir)
        for i in range(10):
            q.put(i)
            assert q.get().value == i

    except:
        raise
    finally:
        shutil.rmtree(tmpdir)


#
# Queue().task_done
#
def test_Queue_task_done():
    """The Queue class has the task_done method."""
    assert hasattr(queue.Queue, 'task_done')


def test_Queue_task_done_calls_ack():
    """Queue.task_done calls reader.ack."""
    try:
        tmpdir = mktemp()
        q = queue.Queue(tmpdir)
        q.put('TEST')
        assert q._reader.status() == {1: False}
        d = q.get()
        assert q._reader.status() == {1: False}
        q.task_done(d)
        assert q._reader.status() == {1: True}
    except:
        raise 
    finally:
        shutil.rmtree(tmpdir)


#
# Queue().join
#
def test_Queue_join():
    """The Queue class has the join method."""
    assert hasattr(queue.Queue, 'join')


def test_Queue_join_waits():
    """Queue.join waits until all is completed."""
    from time import sleep
    from threading import Thread

    try:
        tmpdir = mktemp()

        q = queue.Queue(tmpdir, max_log_events=500000)
        for i in range(100000):
            q.put('TEST')
        q._writer.set_current_log().sync()

        def joining_thread():
            for i in range(100000):
                q.task_done(q.get())


        t = Thread(target=joining_thread)
        t.daemon = True
        t.start()

        assert q._reader.status() == {1: False}
        q.join()
        assert q._reader.status() == {1: True}

        t.join()
    except:
        raise
    finally:
        shutil.rmtree(tmpdir)
