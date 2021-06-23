package com.gsk.kg.engine
package optimizer

import higherkindness.droste.Basis
import com.gsk.kg.engine.data.ChunkedList
import com.gsk.kg.sparqlparser.Expr

import com.gsk.kg.sparqlparser.StringVal

object ReorderBgps {

  def reorderBgps[T](dag: T)(implicit T: Basis[DAG, T]): T = {
    T.coalgebra(dag).rewrite { case DAG.BGP(quads) =>
      DAG.BGP(reorder(quads))
    }
  }

  private def reorder(quads: ChunkedList[Expr.Quad]): ChunkedList[Expr.Quad] = {
    val graph = quads.foldLeft(Graph.empty[Expr.Quad]) { (graph, from) =>
      graph.addEdges(from, findAdjacent(from, quads).toSet)
    }

    println(graph)

    quads
  }

  def findAdjacent(
      first: Expr.Quad,
      list: ChunkedList[Expr.Quad]
  ): Set[Expr.Quad] = {
    val x = list.foldLeft(Set.empty[Expr.Quad]) { (set, second) =>
      if (first != second && shareVariables(first, second)) {
        set + second
      } else {
        set
      }
    }

    x
  }

  def isVariable(sv: StringVal): Boolean =
    sv match {
      case StringVal.VARIABLE(s) => true
      case _ => false
    }

  def shareVariables(a: Expr.Quad, b: Expr.Quad): Boolean =
    Set(a.s, a.p, a.o).filter(isVariable) intersect Set(b.s, b.p, b.o).filter(isVariable) nonEmpty

  def containsVar(variable: StringVal.VARIABLE, quad: Expr.Quad): Boolean =
    variable == quad.s || variable == quad.p || variable == quad.o

}
