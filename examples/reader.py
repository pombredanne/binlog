from itertools import cycle
import atexit
import os
import signal
import time

from binlog.reader import Reader

r = Reader('test', checkpoint='status')

def save_status():
    """
    Save a checkpoint with the current status.
    
    Only the acknowledged entries will be included in the checkpoint.

    """
    r.save()

    
atexit.register(save_status)

for i in cycle(range(1, 20001)):
    if i == 20000:  # Make a checkpoint each 20k reads.
        save_status()

    n = r.next_record()  # Read the next log entry.

    if n is None:
        time.sleep(1)
    else:
        print('.', end='', flush=True)
        r.ack(n)  # Acknowledge the reception of the entry.
