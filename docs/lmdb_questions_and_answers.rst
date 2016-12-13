LMDB: Questions/Answers and Notes
=================================

This questions and notes arose from the reading of lmdb Documentation
Release 0.92 (pdf format).

Installation
------------

`#1`
~~~~

Q: Page 5. Installation with CFFI? Which is better? (speed, compilation, etc)
A: Page 44. [...]The CFFI binding performance is very poor in CPython[...]


Named Databases
---------------

`#2`
~~~~

N: Page 9. Always use sub-databases/named databases to avoid key collisions.


Storage Efficiency & Limits
---------------------------

`#3`
~~~~

N: Page 11. Maybe is better to use hashes as keys up to 511bytes for
   string indexes.


Memory Usage
------------

`#4`
~~~~

N: Page 13. TODO: Document memory usage in binlog.


Buffers
-------

`#5`
~~~~

N: Page 17. Enable buffers=True in all readers and use deserialize ASAP to
   avoid problems.


`WRITEMAP` Mode
---------------

`#6`
~~~~

Q: Page 19. How worst is the performance with writemap=False?


Transaction Management
----------------------

`#7`
~~~~

N: Page 23. Close readers often to allow writers to reuse deleted blocks.


`#8`
~~~~

Q: Page 23. What happens if a process doing a write transaction gets
   killed. Do the database become unusable? How we fix it?


Interface
---------


`#9`
~~~~

N: Page 27. map_size must be exposed in the config file. TODO: Explain
   in docs.


`#10`
~~~~~

N: Page 29. Expose the copy function as a tool.


`#11`
~~~~~

N: Page 29. Expose the info function as a tool.


`#12`
~~~~~

N: Page 30. Do not call open_db inside an existing transaction without
   supplying it as a parameter of the open_db function.


`#13`
~~~~~

N: Page 30. Use dupsort=True in the Index databases.


`#14`
~~~~~

Q: Page 30. What is the impact of dupfixed=True on performance?


`#15`
~~~~~

N: Page 30. Expose the stat function as a tool.


`#16`
~~~~~

N: Page 32. Use drop() to massively delete keys stored in the same database.
   TODO: Check what is the better database policy for this.


`#17`
~~~~~

Q: Page 32. Can put(apppend=True) be used by the writer of the `Entries`
   database? Check performance.


`#18`
~~~~~

N: Page 37. Use put(dupdata=True) in Index databases.


`#19`
~~~~~

N: Page 37. Use put(append=True) in Entries database.


`#20`
~~~~~

Q: Page 37. Use putmulti instead of put in the writer of Entries. Do it
   makes any difference?


Command Line Tools
------------------


`#21`
~~~~~

Q: Page 41. Which is better: make custom tools or use python -mlmdb??
