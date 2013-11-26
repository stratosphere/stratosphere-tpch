TPC-H For Stratosphere
======================

Implementation of all TPC-H queries for Stratosphere, using Scala

TPC-H Specification: http://www.tpc.org/tpch/spec/tpch2.16.0.pdf

Usage
-----

To run TPC-H query X, type

```
stratosphere-tpch QXX [options] <args>...
```

For a full list of the options and arguments supported by each query, type

```
stratosphere-tpch --help
```

Instructions for Developers
---------------------------

To add a new query X:

1. Add a class *eu.stratosphere.tpch.query.TPCHQueryXX* that extends the abstract base class [eu.stratosphere.tpch.query.TPCHQuery](src/main/scala/eu/stratosphere/tpch/query/TPCHQuery.scala) (see [TPCHQuery01](src/main/scala/eu/stratosphere/tpch/query/TPCHQuery01.scala) for a reference of a query implementation).
1. Add the corrsponding case statement in the [TPCHQuery.create()](src/main/scala/eu/stratosphere/tpch/query/TPCHQuery.scala#L48) factory method.