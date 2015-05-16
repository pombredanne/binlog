binlog
======

Multiple writer/reader binary log. Each writer can append messages to
the log and the writers can read them sequencially.

+-----------------+--------------------------------------------------------------------------+-------------------------------------------------------------------------------------+
|                 |                          **Tests**                                       |                                     **Coverage**                                    |
+=================+==========================================================================+=====================================================================================+
|                 | .. image:: https://travis-ci.org/nilp0inter/binlog.svg?branch=master     |  .. image:: https://coveralls.io/repos/nilp0inter/binlog/badge.svg?branch=master    |
|   **Master**    |    :target: https://travis-ci.org/nilp0inter/binlog                      |     :target: https://coveralls.io/r/nilp0inter/binlog?branch=master                 |
|                 |    :alt: Master branch tests status                                      |     :alt: Master branch coverage status                                             |
+-----------------+--------------------------------------------------------------------------+-------------------------------------------------------------------------------------+
|                 | .. image:: https://travis-ci.org/nilp0inter/binlog.svg?branch=develop    |  .. image:: https://coveralls.io/repos/nilp0inter/binlog/badge.svg?branch=master    |
|  **Develop**    |    :target: https://travis-ci.org/nilp0inter/binlog                      |     :target: https://coveralls.io/r/nilp0inter/binlog?branch=develop                |
|                 |    :alt: Develop branch tests status                                     |     :alt: Develop branch coverage status                                            |
+-----------------+--------------------------------------------------------------------------+-------------------------------------------------------------------------------------+


Installation
------------

`binlog` depends on `bsddb3` which in turn depends on `Berkeley DB` (C library).

To be able to install `bsddb3` you need to install `Berkeley DB` first

.. code-block:: bash

   # apt-get install libdb5.1-dev


Also you need to export the environment variable **BERKELEYDB_DIR** with
the path of the installed library. As an example if ``db.h`` is in
``/usr/include/db.h`` you need to set the variable like this:

.. code-block:: bash

   $ export BERKELEYDB_DIR=/usr  # because 'bsddb3' will append 'include/db.h'


You can now finish the installation:

.. code-block:: bash

   $ pip install binlog


Development
-----------

Follow the instructions in the **Installation** section except for the last one.

Clone this package and install the package in develop mode.

.. code-block:: bash

   $ git clone -b develop https://github.com/nilp0inter/binlog
   $ cd binlog
   $ pip install -e .


To finish install the development dependencies:

.. code-block:: bash

   $ pip install -r requirements/tox.txt

And you are ready for run some tests!

.. code-block:: bash

   $ tox
