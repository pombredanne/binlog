import time

from bsddb3 import db

from .constants import MAX_LOG_EVENTS


class Queue:
    def __init__(self, path, max_log_events=MAX_LOG_EVENTS):
        self.path = path
        self.max_log_events = max_log_events
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

    def get_nowait(self):
        try:
            return self._reader.next_record()
        except (db.DBInvalidArgError, ValueError):
            self.__reader = None
            return None

    def get(self):
        r = self.get_nowait()
        while r is None:
            time.sleep(0.1)
            r = self.get_nowait()
        return r

    def put(self, data):
        return self._writer.append(data)

    def task_done(self, data):
        self._reader.ack(data)

    def join(self):
        def all_done(s):
            return all(r for r in s.values())

        s = self._reader.status()
        while not all_done(s):
            time.sleep(0.1)
            s = self._reader.status()
