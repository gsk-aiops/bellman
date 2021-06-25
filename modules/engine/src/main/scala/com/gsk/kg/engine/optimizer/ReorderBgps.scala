package com.gsk.kg.engine
package optimizer

import cats.implicits._

import higherkindness.droste.Basis

import com.gsk.kg.engine.data.ChunkedList
import com.gsk.kg.engine.data.ToTree._
import com.gsk.kg.sparqlparser.Expr
import com.gsk.kg.sparqlparser.StringVal

import quiver._

object ReorderBgps {

  /** This optimization reorders the triples inside a BGP so that we
    * minimize the number of cross joins.
    *
    * The idea is that we put next to each other triples that share
    * variables, since triples that don't end up in cross joins.
    *
    * In order to do that we create a [[quiver.Graph]] of triples in
    * which nodes are triples, and edges exists when these triples
    * share variables.
    *
    * Later on, in order to regenerate the list of triples, we perform
    * a BFS starting from the first triple in the BGP.
    *
    * @param dag
    * @return
    */
  def reorderBgps[T](dag: T)(implicit T: Basis[DAG, T]): T = {
    T.coalgebra(dag).rewrite { case DAG.BGP(quads) =>
      DAG.BGP(reorder(quads))
    }
  }

  def phase[T](implicit T: Basis[DAG, T]): Phase[T, T] = Phase { t =>
    val result = reorderBgps(t)
    (result != t)
      .pure[M]
      .ifM(
        Log.debug(
          "Optimizer(ReorderBGPs)",
          s"resulting query: ${result.toTree.drawTree}"
        ),
        ().pure[M]
      ) *>
      result.pure[M]
  }

  private def reorder(quads: ChunkedList[Expr.Quad]): ChunkedList[Expr.Quad] = {
    val g = quads
      .foldLeft(empty[Expr.Quad, Unit, Unit]) { (graph, quad) =>
        graph & Context(
          findAdjacent(quad, quads),
          quad,
          (),
          findAdjacent(quad, quads)
        )
      }

    if (quads.toList.isEmpty) {
      quads
    } else {
      val ls: Vector[Expr.Quad] = quads.foldLeft(Vector.empty[Expr.Quad]) {
        (list, quad) =>
          g.bfs(quad).foldLeft(list) { (lst, quad) =>
            if (lst.contains(quad)) {
              lst
            } else {
              lst :+ quad
            }
          }
      }

      ChunkedList.fromList(ls.toList)
    }
  }

  def findAdjacent(
      first: Expr.Quad,
      list: ChunkedList[Expr.Quad]
  ): Vector[(Unit, Expr.Quad)] =
    list.foldLeft(Vector.empty[(Unit, Expr.Quad)]) { (set, second) =>
      if (first != second && shareVariables(first, second)) {
        set :+ (() -> second)
      } else {
        set
      }
    }

  def isVariable(sv: StringVal): Boolean =
    sv match {
      case StringVal.VARIABLE(s) => true
      case _                     => false
    }

  def shareVariables(a: Expr.Quad, b: Expr.Quad): Boolean =
    Set(a.s, a.p, a.o).filter(isVariable) intersect Set(b.s, b.p, b.o).filter(
      isVariable
    ) nonEmpty

  def containsVar(variable: StringVal.VARIABLE, quad: Expr.Quad): Boolean =
    variable == quad.s || variable == quad.p || variable == quad.o

}
