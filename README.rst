Upload Service for DD-DeCaF
===========================

|Build Status| |Coverage Status|


.. |Build Status| image:: https://travis-ci.org/DD-DeCaF/upload.svg?branch=master
   :target: https://travis-ci.org/DD-DeCaF/upload
.. |Coverage Status| image:: https://codecov.io/gh/DD-DeCaF/upload/branch/master/graph/badge.svg
   :target: https://codecov.io/gh/DD-DeCaF/upload

Installation
------------

Setup is as easy as typing `make start` in the project directory. However, the
DD-DeCaF upload service depends on a running instance of the iloop backend. By
default it expects a service `iloop-backend` to accept connections on port `80`
on a docker network called `iloop-net`. You can control this and other behavior
by either defining environment variables or writing them to a `.env` file.

+-----------------+--------------------------+--------------------------------+
| Variable        | Default Value            | Description                    |
+=================+==========================+================================+
| ``UPLOAD_PORT`` | ``7000``                 | Exposed port of the upload     |
|                 |                          |  service.                      |
+-----------------+--------------------------+--------------------------------+
| ``ILOOP_API``   | ``iloop-backend:80/api`` | Exposed port of the upload     |
|                 |                          | service.                       |
+-----------------+--------------------------+--------------------------------+
| ``ILOOP_TOKEN`` | ``''``                   | Token for the service to       |
|                 |                          | connect to the iloop backend.  |
|                 |                          | (Not necessary if connecting   |
|                 |                          | via the                        |
|                 |                          | metabolica-ui-frontend.)       |
+-----------------+--------------------------+--------------------------------+

Usage
_____

Type ``make`` in order to see all commonly used commands.
