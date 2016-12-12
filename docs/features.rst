Features
========


Inmutable entries
-----------------

Only two operations are allowed in the database: append and delete.

No register can be modified in any way.


Addressing
----------

Every entry has a unique internal identifier and can not be reused, even
deleting events.

Identifiers are consecutive 64 bit unsigned integers, starting with 0
and ending with 2^64-1=18446744073709551615. This address space
theoretically allows you to continiously write 100000 per second for
5849424 years.

.. note::

   This decision makes the design a lot simpler. The cost is the waste
   of disk space, but is pretty cheap this days.  The address space is
   big enough so can't be exausted during the lifetime of the project. 


Indexes
-------


Reader processing acknowledge
-----------------------------

Each reader must register itself in the system. Readed entries must be
acknowledged by each reader when the processing is done. This acknowledge
have a type.  Acknowledge entries can be deleted by the system depending
on the deletion policy.


Deletion policy
---------------

Only ackownoledged entries can be deleted.

