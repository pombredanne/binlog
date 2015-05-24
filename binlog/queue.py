from queue import Empty
import time

from bsddb3 import db

from .constants import MAX_LOG_EVENTS


class Queue:
    def __init__(self, path, max_log_events=MAX_LOG_EVENTS, autocommit=False):
        self.path = path
        self.max_log_events = max_log_events
        self.autocommit = autocommit

        self.__reader = None
        self.__writer = None

    @property
    def _reader(self):
        if self.__reader is None:
            from binlog.reader import Reader
            self.__reader = Reader(self.path)
        return self.__reader

    @property
    def _writer(self):
        if self.__writer is None:
            from binlog.writer import Writer
            self.__writer = Writer(self.path,
                                   max_log_events=self.max_log_events)
        return self.__writer

    def _get(self):
        try:
            return self._reader.next_record()
        except (db.DBInvalidArgError, ValueError):
            self.__reader = None
            return None

    def get_nowait(self):
        data = self._get()

        if data is None:
            raise Empty
        else:
            return data

    def get(self, block=True, timeout=None):
        r = self._get()
        if block:
            wait = 0
            while r is None:
                time.sleep(0.1)
                if timeout:
                    wait += 0.1
                    if wait >= timeout:
                        break
                r = self._get()

        if r is None:
            raise Empty
        else:
            return r

    def put(self, data):
        r = self._writer.append(data)
        if self.autocommit:
            self._writer.set_current_log.sync()
        return r

    def task_done(self, data):
        self._reader.ack(data)
        if self.autocommit:
            self._reader.save()

    def join(self, timeout=None):
        def all_done(s):
            return all(r for r in s.values())

        s = self._reader.status()
        wait = 0
        while not all_done(s):
            if timeout:
                if wait < timeout:
                    time.sleep(0.1)
                    wait += 0.1
                else:
                    raise TimeoutError()
            s = self._reader.status()
