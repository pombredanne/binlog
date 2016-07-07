import contextlib
import os
import shutil
import subprocess
import sys
import tempfile
import time

import pytest

from binlog import binlog
from binlog import reader, writer

# Binlog implementations.
BINLOG_IMPL = [binlog.TDSBinlog, binlog.CDSBinlog]

# Reader & Writer implementations.
RW_IMPL = [
    (reader.TDSReader, writer.TDSWriter),
    (reader.CDSReader, writer.CDSWriter)]


@pytest.yield_fixture(autouse=True)
def server_factory():

    with tempfile.TemporaryDirectory() as base:

        socket_path = os.path.join(base, 'binlog.sock')
        env_path = base

        @contextlib.contextmanager
        def _server_factory():
            # env_path could be reused by hypothesis so we need to
            # remove all the files.
            shutil.rmtree(env_path)
            os.mkdir(env_path)

            s = None
            try:
                s = subprocess.Popen(["binlog", env_path, socket_path],
                                     stdout=sys.stdout,
                                     stderr=subprocess.STDOUT)

                while not os.path.exists(socket_path):
                    time.sleep(1)

                yield (socket_path, env_path)
            finally:
                time.sleep(1)  # Wait before closing the server to let all the data arrive.
                if s is not None:
                    s.terminate()
                    s.wait()

        yield _server_factory
