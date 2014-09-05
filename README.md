# Overview

cl-mongo-stream is a stream-oriented implementation of the [MongoDB wire protocol](http://docs.mongodb.org/meta-driver/latest/legacy/mongodb-wire-protocol/) for Common Lisp, currently running only on LispWorks due to a number of LispWorks-specific optimizations.

This library provides support for reading and writing [BSON documents](http://bsonspec.org) to and from Common Lisp, including both a functional interface to produce and parse BSON documents on the fly, as well as a representation of BSON documents as Lisp property lists or association lists. BSON support is provided in its own BSON package because it may be useful independent from MongoDB.

The library also provides support for the essential MongoDB wire protocol operations in the cl-mongo-stream package. There is also some basic support for some basic CRUD and other commands, but that part of the library is far from complete. The focus is on support for BSON documents and the wire protocol.

Please see [elprep-mongo](https://github.com/ExaScience/elprep-mongo) for an example of how this library could be used.

## Dependencies

The BSON implementation depends on the named-readtables library. The cl-mongo-stream further depends on the string-case library. Both libraries are available through the [quicklisp](http://www.quicklisp.org) package manager.
