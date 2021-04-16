---
layout: docs
title: Graph Transformation
permalink: docs/phases/transformToGraph
---

## Graph transformation

The datatype that the parser returns, `Expr` is not very well suited to running into Spark DataFrames.  In order to simplify compilation into Spark DataFrames, we transform values from Expr datatype into our internal DAG.

The internal DAG has several advantages:

- It contains queries as cases of the ADT, not as an external Query object.
- Avoids duplication of some SparQL algebrae.