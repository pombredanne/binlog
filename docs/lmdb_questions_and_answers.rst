LMDB: Questions/Answers and Notes
=================================

This questions and notes arose from the reading of lmdb Documentation
Release 0.92 (pdf format).

1. Installation
---------------

- `#1.1`

  - **Question:** Page 5. Installation with CFFI? Which is better?
    (speed, compilation, etc)

  - **Answer:** Page 44. [...]The CFFI binding performance is very poor
    in CPython[...]


2. Named Databases
------------------

- `#2.1`

  - **Note:** Page 9. Always use sub-databases/named databases to avoid
    key collisions.


3. Storage Efficiency & Limits
------------------------------

- `#3.1`

  - **Note:** Page 11. Maybe is better to use hashes as keys up to
    511bytes for string indexes.


4. Memory Usage
---------------

- `#4.1`

  - **Note:** Page 13. TODO: Document memory usage in binlog.


5. Buffers
----------

- `#5.1`

  - **Note:** Page 17. Enable buffers=True in all readers and use
    deserialize ASAP to avoid problems.


6. `WRITEMAP` Mode
------------------

- `#6.1`

  - **Question:** Page 19. How worst is the performance with
    writemap=False?

  - **Answer:** Actually in slower with writemap=True.


7. Transaction Management
-------------------------

- `#7.1`

  - **Note:** Page 23. Close readers often to allow writers to reuse
    deleted blocks.


- `#7.2`

  - **Question:** Page 23. What happens if a process doing a write
    transaction gets killed. Do the database become unusable? How we fix
    it?

  - **Answer:** Using tests/lmdb_questions/test_7_2.py we create a process
    start a transaction and kill the process in the middle. No problems
    were observed reading and writing again in the DB.


8. Interface
------------


- `#8.1`

  - **Note:** Page 27. map_size must be exposed in the config file.
    TODO: Explain in docs.

- `#8.2`

  - **Note:** Page 29. Expose the copy function as a tool.

- `#8.3`

  - **Note:** Page 29. Expose the info function as a tool.

- `#8.4`

  - **Note:** Page 30. Do not call open_db inside an existing
    transaction without supplying it as a parameter of the open_db
    function.

- `#8.5`

  - **Note:** Page 30. Use dupsort=True in the Index databases.

- `#8.6`

  - **Question:** Page 30. What is the impact of dupfixed=True on performance?

  - **Answer:** 

- `#8.7`

  - **Note:** Page 30. Expose the stat function as a tool.

- `#8.8`

  - **Note:** Page 32. Use drop() to massively delete keys stored in the
    same database.  TODO: Check what is the better database policy for
    this.

- `#8.9`

  - **Question:** Page 32. Can put(append=True) be used by the writer
    of the `Entries` database? Check performance.

  - **Answer:** Can be used in Entries database since the records are
    secuential.  The performance benefit is bigger as database get more
    and more keys.

- `#8.10`

  - **Note:** Page 37. Use put(dupdata=True) in Index databases.

- `#8.11`

  - **Note:** Page 37. Use put(append=True) in Entries database.

- `#8.12`

  - **Question:** Page 37. Use putmulti instead of put in the writer of
    Entries.  Does it makes any difference?

  - **Answer:** tests/lmdbquestions/test_8_12{a,b}.py were used to
    measure the writting of 100,000 events to disks. The method with
    putmulti performed slightly better (3 runs average: 10.626 secs vs
    10.323 secs) than multiple calls to put and the code is cleaner this
    way.


- `#8.13`

  - **Question:** Page 30. Can `integerkey` be used with UINT64?

  - **Answer:**  


9. Command Line Tools
---------------------


- `#9.1`

  - **Question:** Page 41. Which is better: make custom tools or use
    python -mlmdb??
