import lmdb
import random
import struct

MAX_EVENTS = 100000000

env = lmdb.open('/tmp/test', map_size=214748364700, max_dbs=2)

def to_key(i):
    return struct.pack('!Q', i)

def from_key(k):
    return struct.unpack('!Q', k)[0]

with env.begin(write=True) as txn:
    metadb = env.open_db(key=b'meta', txn=txn)
    entriesdb = env.open_db(key=b'entries', txn=txn)

end_key = 0

while end_key < MAX_EVENTS:
    num_events_to_write = random.randint(1000000, 10000000)
    if num_events_to_write + end_key > MAX_EVENTS:
        num_events_to_write = MAX_EVENTS - end_key - 1
    with env.begin(write=True) as txn:
        with txn.cursor(metadb) as metacursor:
            next_event_idx = from_key(metacursor.get(b'next_event_idx',
                                                     default=to_key(0)))

            start_key = next_event_idx
            end_key = next_event_idx + num_events_to_write

            print(num_events_to_write, start_key, end_key)
            items = ((to_key(k), bytes(512))
                     for k in range(start_key, end_key))
            with txn.cursor(entriesdb) as entriescursor:
                for key, value in items:
                    (consumed, added) = entriescursor.putmulti(items,
                                                               dupdata=False,
                                                               overwrite=False,
                                                               append=False)
                if consumed != added:
                    raise RuntimeError("Key already exists in database")

            metacursor.put(b'next_event_idx', to_key(end_key + 1))
