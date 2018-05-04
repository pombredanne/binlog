import os
import pickle
import sys

from binlog import model
from binlog.databases import Registry


def migrate_model(path):
    print("Migrating %s..." % path)

    with model.Model.open(path) as db:
        with db.readers() as readers:
            with readers.txn.cursor(readers.db["Checkpoints"]) as cursor:
                for k, v in cursor.iternext():
                    name = bytes(k).replace(b'\x00', b'.').decode('utf-8')
                    print("Loading %s... " % name, end="")
                    oldregistry = pickle.loads(v)
                    print("%d values..." % len(oldregistry.acked), end="")
                    with Registry.named(name).cursor(readers) as rcursor:
                        for s in oldregistry.acked:
                            rcursor.put(s.R, s.L)
                    print("DONE")

            readers.txn.drop(readers.db["Checkpoints"], delete=True)
    print("ALL DONE!")


def usage():
    print("Usage: %s <path_to_binlog>" % sys.argv[0])
    sys.exit(1)


if __name__ == '__main__':
    try:
        path = sys.argv[1]
    except IndexError:
        usage()
    else:
        if not os.path.isdir(path):
            usage()
        else:
            migrate_model(path)
