package com.gsk.kg.engine.optimizer

final case class Graph[T](rep: Map[T, Set[T]]) {

  def addNode(t: T): Graph[T] =
    if (rep.contains(t))
      this
    else
      Graph(rep + (t -> Set.empty))

  def addEdge(from: T, to: T): Graph[T] =
    if (rep.contains(from) && rep(from).contains(to))
      this
    else if(rep.contains(from))
      Graph(rep.updated(from, rep(from) + to))
    else
      Graph(rep + (from -> Set(to)))

  def addEdges(from: T, to: Set[T]): Graph[T] =
    if (! rep.contains(from))
      Graph(rep + (from -> to))
    else
      Graph(rep + (from -> rep(from).union(to)))

}

object Graph {
  def empty[T] = Graph(Map.empty[T, Set[T]])
}
