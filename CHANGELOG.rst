CHANGELOG
=========

5.1.1
-----

- The readers module now contains a set of reserved readers names which can't
  be used. These reader names are now filtered from `list_readers` and not
  allowed to be created nor removed.


5.1.0
-----

- ANDIterSeek and ORIterSeek reimplemented in a non-recursive way.
- Improved purge(). Now it doesn't try to remove all previously removed entries.


5.0.0
-----

- Registries implemented as lmdb databases.
- New method connection.ack_from_filter.
- Migration tool "python -m binlog.migrateregistry".


4.2.0
-----

- New method clone_reader to create a new reader with the content of an
  existing one.


4.1.0
-----

- Added function `reset_connections` under `binlog.connectionmanager` module to
  reset the per process connection cache. This function will be called
  automatically on Python 3.7 on children processes after forks. Older versions
  of Python MUST call it explicitly.


4.0.0
-----

- Removed compatibility code from version <= 2


3.3.0
-----

- Preventing the user to use the same connection in different processes.


3.2.0
-----

- Implementation of per-process connection manager to reuse connections
  when used concurrently.


3.1.0
-----

- Preventing the user to picklelize connections.
- Preventing the user to use a closed connection.


3.0.3
-----

- Preventing the user to use the library in a multithreading
  environment.


3.0.2
-----

- Stopping the Connection class from opening and closing the lmdb
  environment on every single operation.


3.0.1
-----

- Return True on recursive_ack if at least one reader success on the ack.


3.0.0
-----

- Rebuild from scratch now using LMDB instead of BerkeleyDB.
- Get rid of the UNIX socket server.
- Better reader semantics.
- Indexes.
- Django-like model definitions.
- Transparent serialization using pickle.


1.2.0
-----

- What 1.1.0 says...


1.1.0
-----

- Do not store a new row in server mode when the socket is opened and
  closed without any data.


1.0.1
-----

- Better server stopping method.


1.0.0
-----

- Removed the serialization method. Now the clients must provide their
  own.
- UNIX socket server.


0.0.4
-----

- New backend using Berkeley's Concurrent Data Storage (CDS).
- Specialized Reader and Writer for each backend (TDS or CDS).


0.0.3
-----

- Skip opening errors when databases are deleted in the status method.  Issue #4
- Skip register values not present in the logindex. Issue #5
- Using transaction in order to not lock the writer on deletions. Issue #6.


0.0.2
-----

- Reader, Writer and Queue classes.


0.0.1
-----

- Initial commit.
