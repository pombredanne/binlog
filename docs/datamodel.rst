Data Model
==========

The Binlog class
----------------

Any subclass of the `Binlog` class represents a way of storing entries
in a binary log (a.k.a model). The instances of this subclasses
represents entries. These entries can be commited (already stored in the
log) or not.


LMDB Environments
-----------------

The `Data` environment
~~~~~~~~~~~~~~~~~~~~~~

The `Config` database
+++++++++++++++++++++

This database store metadata about the `Entries` database.

`next_entry_id`: ID of the next entry to be stored in the log.


The `Entries` database
++++++++++++++++++++++

One entry is stored in this database for each entry in binlog. Each
entry is a pair (key, value) of the following form:

* key = 64 bit unsigned integer (bigendian)
* value = pickle


The `Index` databases
+++++++++++++++++++++

Set of databases storing indexes for the `Entries` database. A database
is created for each index with the following naming scheme: TBD.


The `Checkpoint` environment
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The `Readers` database
++++++++++++++++++++++

In this database readers store a register of which entries of the
`Entries` database are readed and successfully processed. There is one
or more entries in the `Reader` database environment for each reader in the
system in the form '{reader_name}::{key}'.


.. todo::

   A better explaination of subregisters.

   One reader may have one or more entries in the database to store
   different views of the ack status for its internal use. 

