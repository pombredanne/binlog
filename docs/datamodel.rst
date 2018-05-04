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


The `Readers` environment
~~~~~~~~~~~~~~~~~~~~~~~~~

In this environment there is a database for each registered reader.

Each database consists on entries which keys and values represent the end and
the start (respectively) of readed `Entries` segments.

For example, if a reader database contains:

  +-----+-------+
  | Key | Value |
  +-----+-------+
  | 10  | 0     |
  | 25  | 15    |
  +-----+-------+

This reader has already readed and acked entries 0 to 10 and 15 to 25 of the
`Entries` database.
