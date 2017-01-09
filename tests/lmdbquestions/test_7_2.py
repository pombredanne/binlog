# What happens if a process doing a write transaction gets killed. Do
# the database become unusable? How we fix it?

import os
import signal
import lmdb

env = lmdb.open('/tmp/test', map_size=2147483647)
with env.begin(write=True) as txn:
    cursor = txn.cursor()
    os.kill(os.getpid(), signal.SIGKILL)
