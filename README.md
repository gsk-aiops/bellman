# Bellman

Bellman executes SparQL queries in Spark.

## Developer documentation

There's some documentation for developers in the microsite <https://gsk-aiops.github.io/bellman/>.

## Modules

### Algebra parser

A parser for converting Sparql queries to Algebraic data types. These
ADTs can later be used to generate queries for the target system.

### Spark engine

The Spark engine runs after the algebra parser, and produces Spark
jobs that execute your SparQL queries.

## Publishing

In order to publish a new version of the project one must create a new
release in Github.  The release version must be of the format `v*.*.*`.

Snapshots of the project are published to Sonatype snapshots on every
merge to master as well.

## RDF Tests

RDF tests are integrated in bellman, in the `bellman-rdf-tests` module.
One can run the tests by using the `sbt bellman-rdf-tests/test` command.

