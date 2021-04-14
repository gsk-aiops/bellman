---
layout: docs
title: Compilation
permalink: docs/compilation
---

# Compilation

The process of querying the underlying Spark datasest is dictated by SparQL algebra.  Bellman expects a three (or four) column dataset representing a graph, in which the first column represents the _subject_,the second one the _predicate_, and the third one the _object_. It's optional to add a fourth column representing the _graph_ to which the edge belongs.

What we do in Bellman is, for each triple in the **Basic Graph Pattern**, query the dataframe, and then join them given their common variables, for example:

```sparql
SELECT ?d ?author
WHERE {
  ?d rdf:type doc:Document .
  ?d doc:Author ?author
}
```

This query selects documents with their author.

In Bellman we query once per triple, so starting with the first triple:

```sparql
?d rdf:type doc:Document
```

We will query the dataframe like this:

```scala
val triple1Result = df.select($"p" === "rdf:type" &&& $"o" === "doc:Document")
```

And for the second triple we  will do:

```scala
val triple2Result = df.select($"$p" === "doc:Author")
```

Finally, with these two values, we will join them on the common variables, in this case the `?d` column only:

```scala
val result = triple1Result.join(triple2Result, "s")
```
