import unittest
import shutil
from collections import namedtuple
from tempfile import mktemp

from hypothesis import find, Settings, Verbosity
from hypothesis.stateful import RuleBasedStateMachine, Bundle, rule
import hypothesis.strategies as st
import pytest

from binlog import reader, writer


class ReadAndWrite(RuleBasedStateMachine):

    @rule(x=st.integers())
    def append(self, x):
        self.writer.append(x)
        self.i_appended.append(x)
        return x

    @rule()
    def read(self):
        n = self.my_reader.next_record()
        if n is not None:
            self.i_read.append(n.value)
            self.i_read_records.append(n)
            self.my_reader.ack(n)
            self.my_reader.save()

    @rule()
    def my_reader_change(self):
        self.my_reader = reader.Reader(tmpdir, checkpoint='test')

    @rule()
    def check_integrity(self):
        assert set(self.i_appended) >= set(self.i_read)
        assert sorted(set(self.i_read_records)) == sorted(self.i_read_records)


class TestReadAndWrite(ReadAndWrite.TestCase):

    def setUp(self):
        self.tmpdir = mktemp()

        self.writer = writer.Writer(self.tmpdir)
        self.writer.append('start')
        self.writer.get_current_log().sync()

        self.my_reader = reader.Reader(self.tmpdir, checkpoint='test')
        self.my_reader.next_record()

        self.i_appended = []
        self.i_read = []
        self.i_read_records = []

    def tearDown(self):
        shutil.rmtree(self.tmpdir)


if __name__ == '__main__':
    unittest.main()
