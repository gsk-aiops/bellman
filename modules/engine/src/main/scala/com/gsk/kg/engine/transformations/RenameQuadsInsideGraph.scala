package com.gsk.kg.engine.transformations

import higherkindness.droste.Basis

import com.gsk.kg.engine.DAG
import com.gsk.kg.engine.DAG.Scan
import com.gsk.kg.engine.DAG._
import com.gsk.kg.sparqlparser.StringVal.URIVAL

/** Rename the graph column of quads inside a graph statement, so the graph column will contain the graph that is being
  * queried on the subtree DAG. Eg:
  *
  * Initial DAG without renaming:
  * Project
  *  |
  *  +- List(VARIABLE(?mbox), VARIABLE(?name))
  *  |
  *  `- Project
  *     |
  *     +- List(VARIABLE(?mbox), VARIABLE(?name))
  *     |
  *     `- Scan
  *        |
  *        +- http://example.org/alice
  *        |
  *        `- BGP
  *           |
  *           `- ChunkedList.Node
  *              |
  *              +- NonEmptyChain
  *              |  |
  *              |  `- Quad
  *              |     |
  *              |     +- ?x
  *              |     |
  *              |     +- http://xmlns.com/foaf/0.1/name
  *              |     |
  *              |     +- ?name
  *              |     |
  *              |     `- *g
  *              |
  *              `- NonEmptyChain
  *                 |
  *                 `- Quad
  *                    |
  *                    +- ?x
  *                    |
  *                    +- http://xmlns.com/foaf/0.1/mbox
  *                    |
  *                    +- ?mbox
  *                    |
  *                    `- *g
  *
  * DAG when renamed quads inside graph statement:
  *  Project
  *  |
  *  +- List(VARIABLE(?mbox), VARIABLE(?name))
  *  |
  *  `- Project
  *     |
  *     +- List(VARIABLE(?mbox), VARIABLE(?name))
  *     |
  *     `- Scan
  *        |
  *        +- http://example.org/alice
  *        |
  *        `- BGP
  *           |
  *           `- ChunkedList.Node
  *              |
  *              +- NonEmptyChain
  *              |  |
  *              |  `- Quad
  *              |     |
  *              |     +- ?x
  *              |     |
  *              |     +- http://xmlns.com/foaf/0.1/name
  *              |     |
  *              |     +- ?name
  *              |     |
  *              |     `- http://example.org/alice
  *              |
  *              `- NonEmptyChain
  *                 |
  *                 `- Quad
  *                    |
  *                    +- ?x
  *                    |
  *                    +- http://xmlns.com/foaf/0.1/mbox
  *                    |
  *                    +- ?mbox
  *                    |
  *                    `- http://example.org/alice
  */
object RenameQuadsInsideGraph {

  def apply[T](implicit T: Basis[DAG, T]): T => T = { t =>
    T.coalgebra(t).rewrite { case s @ Scan(graph, expr) =>
      val projectedSubtree: DAG[T] = T.coalgebra(expr) match {
        case BGP(quads) =>
          BGP(quads.flatMapChunks(_.map(_.copy(g = URIVAL(graph)))))
        case _ => s
      }
      Scan(graph, T.algebra(projectedSubtree))
    }
  }

//  def apply[T](implicit T: Basis[DAG, T]): T => T = { t =>
//    def projectedSubtree(s: Scan[T])(expr: T, graph: String): DAG[T] =
//      T.coalgebra(expr) match {
//        case BGP(quads) =>
//          BGP(quads.flatMapChunks(_.map(_.copy(g = URIVAL(graph)))))
//        case Join(l, r) =>
//          Join(projectedSubtree(s)(l, graph), projectedSubtree(s)(r, graph))
//        case Union(l, r) =>
//          Union(projectedSubtree(s)(l, graph), projectedSubtree(s)(r, graph))
//        case LeftJoin(l, r, filters) =>
//          LeftJoin(
//            projectedSubtree(s)(l, graph),
//            projectedSubtree(s)(r, graph),
//            filters
//          )
//        case _ => s
//      }
//
//    T.coalgebra(t).rewrite { case s @ Scan(graph, expr) =>
//      val projectedSubtree: DAG[T] = projectedSubtree(s)(expr, graph)
//      Scan(graph, T.algebra(projectedSubtree))
//    }
//  }
}
