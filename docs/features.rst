Features
========


Inmutable entries
-----------------

Only two operations are allowed in the `Entries` database: append and delete.

No entry can be modified in any way.


Addressing
----------

Every entry has a unique internal identifier and can not be reused, even
deleting events.

Identifiers are consecutive 64 bit unsigned integers, starting with 0
and ending with 2^64-1=18446744073709551615. This address space
theoretically allows you to continiously write 1000000 per second for
584942 years.

.. note::

   This decision simplify the design. The cost is the waste of disk
   space, but is pretty cheap these days.  The address space is big
   enough so can't be exausted during the lifetime of the project. 


Indexes
-------



Reader processing acknowledge
-----------------------------



Deletion policy
---------------

Only entries acknownledged by all readers can be deleted.
