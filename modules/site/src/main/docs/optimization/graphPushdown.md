---
layout: docs
title: Graph Pushdown
permalink: docs/optimization/graphPushdown
---

# Graph pushdown

There's a feature in SparQL that allows users to query only a specific graph, not the whole graph.  One can perform that using named graphs and the GRAPH statement, here's an example:

```sparql
SELECT *
FROM NAMED <http://example.com/named-graph>
{
  GRAPH <http://example.com/named-graph> {
    ?s ?p ?o
  }
}
```

By default GRAPH is captured as a node in the AST but what we do is treat the graph as another component of triples, making them Quads.

In our Graph Pushdown optimization we send graph information to the quad level, removing the GRAPH node from the AST.

<img src="/img/graph-pushdown.gif" width="100%">

We use that information as any other element from the triple when querying the underlying DataFrame.