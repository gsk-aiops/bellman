package com.gsk.kg.engine
package optimizer

import com.gsk.kg.engine.data.ToTree._
import com.gsk.kg.engine.data.ChunkedList._

import cats.implicits._
import cats.arrow.Arrow

import higherkindness.droste.Basis
import higherkindness.droste.syntax.all._
import com.gsk.kg.engine.DAG._
import com.gsk.kg.sparqlparser.Expr


/**
  * The idea behind this optimization step is to compact BGPs into
  * smaller number of chunks, so that when we query the DataFrame in
  * the [[Engine]], a smaller number of queries is done (number of
  * queries is 1 per [[ChunkedList.Chunk]]).
  *
  * Given an initial BGP like this:
  *
  * ```
  * ?d <http://qwer.com> <http://qwer.com> .
  * ?d <http://test.com> <http://test.com>
  * ```
  *
  * Which is represented as this DAG tree:
  *
  * {{{
  *  BGP
  *  |
  *  `- ChunkedList.Node
  *     |
  *     +- NonEmptyChain
  *     |  |
  *     |  `- Triple
  *     |     |
  *     |     +- ?d
  *     |     |
  *     |     +- http://qwer.com
  *     |     |
  *     |     `- http://qwer.com
  *     |
  *     `- NonEmptyChain
  *        |
  *        `- Triple
  *           |
  *           +- ?d
  *           |
  *           +- http://test.com
  *           |
  *           `- http://test.com
  *
  * }}}
  *
  * We compact the triples into a single chunk:
  *
  * {{{
  * BGP
  * |
  * `- ChunkedList.Node
  *    |
  *    `- NonEmptyChain
  *       |
  *       +- Triple
  *       |  |
  *       |  +- ?d
  *       |  |
  *       |  +- http://test.com
  *       |  |
  *       |  `- http://test.com
  *       |
  *       `- Triple
  *          |
  *          +- ?d
  *          |
  *          +- http://qwer.com
  *          |
  *          `- http://qwer.com
  * }}}
  */
object CompactBGPs {

  def apply[T](implicit T: Basis[DAG, T]): T => T = { t =>
    T.coalgebra(t).rewrite {
      case x @ BGP(triples) =>
        BGP(
          triples.compact({
            case t @ Expr.Triple(_, _, _) => t.getVariables.map(x => x._2 + "->" + x._1.s).mkString(";")
          })
        )
    }
  }

}
