CHANGELOG
=========

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
