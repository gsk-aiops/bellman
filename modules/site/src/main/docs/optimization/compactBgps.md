---
layout: docs
title: compactBgps
permalink: docs/optimization/compactBgps
---

# BGP Compaction

Basic Graph Patterns are the sets of triples that we write on queries to express the edges of the graph we want to focus on, for example:

```sparql
?d a <http://example.com/Document> .
?d <http://example.com/HasSource> <http://example.com/MySource>
```

In this case these two triples express we want to get nodes that have the type `doc:Document`, and that have a specific source.

In order to fulfill this query, we would need to perform two queries to the underlying dataset, and then join the results by the shared variables (?d in this case).

What we'll do to perform BGP compaction is to store the triples in the BGP into a special purpose data structure called `ChunkedList`, that has a `compact` operation.

## Introducing ChunkedList

ChunkedList is a special purpose data structure that looks like a linked list from the outside, with a head and a tail.  The trick we do is that we put elements not directly in the head, but inside other linear data structure from Cats, called `NonEmptyChain`.  We use NonEmptyChain as our Chunk type because it has efficient prepends and appends, and we need that in the implementation.  It looks like this:

```scala
sealed trait ChunkedList[A]
object ChunkedList {
  type Chunk[A] = NonEmptyChain[A]
  val Chunk = NonEmptyChain

  final case class Empty[A]() extends ChunkedList[A]
  final case class NonEmpty[A](head: Chunk[A], tail: ChunkedList[A])
      extends ChunkedList[A]
}
```

The difference that our ChunkedList has from other data structures comes from the `compact` method.  `compact` will try to merge elements into the same chunk given an auxiliary function.  The signature looks like this:

```scala
  def compact[B](f: A => B)(implicit B: Eq[B]): ChunkedList[A] = ???
```

`compact` has some nice properties too.  An already compacted list cannot be further compacted, for example, we can express that with a law like this:

```scala
val list: ChunkedList[Int] = ???

list.compact(fn).compact(fn) === list.compact(fn)
```

Here we can see how ChunkedList compaction looks.  In this example we're compacting it by the first letter of each element, making it group into two chunks, those elements starting with "a", and those starting with "b".

```scala
val list: ChunkedList[String] =
  ChunkedList.fromList(List("a", "as", "asterisk", "b", "bee", "burrito"))

list.compact(_.head)
```

<img src="/bellman/img/chunkedlist.gif">

## Compacting BGPs by shared variables

The auxiliary function we have for compacting BGPs looks for shared variables in the same positions in the triples.

<img src="/bellman/img/bgp-compaction.gif" width="100%">

Now that we have the triples compacted in the BGP, what we do is that we query the underlying datafram once **per chunk**, not once per triple.  You can take a look at the `Engine` file to see how we iterate over all chunks to query the dataframe.