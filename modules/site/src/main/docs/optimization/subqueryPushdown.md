---
layout: docs
title: Subquery Pushdown
permalink: docs/optimization/subqueryPushdown
---

# Subquery pushdown

SparQL supports nesting queries, this is called sub-queries. Eg:

````sparql
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
PREFIX ex: <http://example.org/>

CONSTRUCT {
  ?y foaf:knows ?name .
} 
FROM NAMED <http://example.org/alice>
WHERE {
  ?y foaf:knows ?x .
  GRAPH ex:alice {
    {
      SELECT ?x ?name
      WHERE {
        ?x foaf:name ?name . 
      }
    }
  }
}
````

In this example, in order to support sub-queries when the inner query is evaluated,
the SELECT statement that is mapped as a **Project** in the AST will drop all columns
that are not within the variable bindings ``?x``, ``?name``.

This means that the hidden column for the graph is dropped causing this info to be
lost and consequently, when evaluated the outer query, it will fail as it expects this
column to exist in the sub-query result.

So the strategy is to add the graph column variable to the list of variables that
the inner **Project** will have, this way the graph column is not dropped anymore.

<img src="/bellman/img/subquery-pushdown.gif" width="100%">

Consider that the addition of this graph variable can only be done to the inner
queries, while the variable list of the outer query must remain as it is, if not
the result would contain the graph column and that is not what the user expect
from the query.

This will apply to all nodes that contains **Ask**, **Project** or **Construct**.