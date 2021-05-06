package com.gsk.kg.engine
package optimizer

import higherkindness.droste.data.Fix

import com.gsk.kg.engine.DAG._
import com.gsk.kg.engine.data.ChunkedList
import com.gsk.kg.engine.scalacheck.ChunkedListArbitraries
import com.gsk.kg.engine.scalacheck.DAGArbitraries
import com.gsk.kg.engine.scalacheck.DrosteImplicits
import com.gsk.kg.sparqlparser.Expr
import com.gsk.kg.sparqlparser.StringVal.GRAPH_VARIABLE
import com.gsk.kg.sparqlparser.StringVal.STRING

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class JoinBGPsSpec
    extends AnyFlatSpec
    with Matchers
    with ScalaCheckDrivenPropertyChecks
    with DrosteImplicits
    with DAGArbitraries
    with ChunkedListArbitraries {

  type T = Fix[DAG]
  val T = higherkindness.droste.Project[DAG, T]

  "JoinBGPs" should "rewrite JOIN(BGP(t1), BGP(t2)) into BGP(t1 ++ t2)" in {
    val join: T = joinR(
      bgpR(
        ChunkedList(
          Expr
            .Quad(
              STRING("one"),
              STRING("two"),
              STRING("three"),
              GRAPH_VARIABLE :: Nil
            )
        )
      ),
      bgpR(
        ChunkedList(
          Expr.Quad(
            STRING("four"),
            STRING("five"),
            STRING("six"),
            GRAPH_VARIABLE :: Nil
          )
        )
      )
    )

    val joinsAsBGP: PartialFunction[DAG[T], DAG[T]] = { case j @ Join(l, r) =>
      (T.coalgebra(l), T.coalgebra(r)) match {
        case (BGP(tl), BGP(tr)) => bgp(tl concat tr)
        case _                  => j
      }
    }

    T.coalgebra(join).rewrite(joinsAsBGP) shouldEqual bgpR(
      ChunkedList(
        Expr
          .Quad(
            STRING("one"),
            STRING("two"),
            STRING("three"),
            GRAPH_VARIABLE :: Nil
          ),
        Expr.Quad(
          STRING("four"),
          STRING("five"),
          STRING("six"),
          GRAPH_VARIABLE :: Nil
        )
      )
    )
  }
}
