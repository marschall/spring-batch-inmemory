Spring Batch In Memory
======================

In memory implementations of the Spring Batch `JobRepository` and `JobExplorer` interfaces as the map based implementations are deprecated.


Implementation Notes
--------------------

As this is intended for testing purposes the implementation is based on `HashMap`s rather than `ConcurrentHashMap` in order to save memory. Thread safety is instead provided through a `ReadWriteLock`.

