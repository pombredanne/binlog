Data Model
==========


LMDB Environments
-----------------

The `Data` environment
~~~~~~~~~~~~~~~~~~~~~~

The `Meta` database
+++++++++++++++++++

This database store metadata about the `Entries` database.


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

The `Reader` databases
++++++++++++++++++++++

In this database readers store a register of which entries of the
`Entries` database are readed and successfully processed. There is one
`Reader` database in the `Checkpoint` environment for each reader in the
system.

