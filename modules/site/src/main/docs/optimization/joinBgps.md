---
layout: docs
title: compactBgps
permalink: docs/optimization/joinBgps
---

# Join BGPs

In SparQL, when you write a BGP with several triples, there's an implicit JOIN operation between them

```sparql
SELECT ?person ?acuaintance
WHERE {
  ?person rdf:type foaf:Person .
  ?person foaf:knows ?acquaintance
}
```

In this query, we're asking for all aquaintances for persons, and under the hood we're first querying for all triples that have `p == "rdf:type"` and `o == foaf:Person` (effectively looking for all persons), and then we're querying for all triples that have `p == "foaf:knows"`.  Once we have both results, we join them using a JOIN by their common variables, `?person` in this case.

However, there are cases in which a `JOIN` node is introduced.  `JOIN` may be useful in some cases, such as when joinin the results of a query to the default graph with results from a named graph.  But for BGPs it doesn't make a lot of sense, so what we do is removing the JOIN node and just joining all triples from both BPGs into one:

<img src="/bellman/img/join-bgps.gif" width="100%">

You can see in the figure above that there is a JOIN node that ties together the BGP from the named graph and the one for the default graph.  After our [graph pushdown](/bellman/docs/optimization/graphPushdown) optimization it doesn't make sense anymore, so we remove that node by joining the BGPs into a single one.  This optimization opens the door to further performance improvements because once you put all triples into the same BGP, it becomes subject to [BGP compaction](/bellman/docs/optimization/compactBgps).