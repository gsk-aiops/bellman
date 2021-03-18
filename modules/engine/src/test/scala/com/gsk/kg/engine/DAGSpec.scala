package com.gsk.kg.engine

import higherkindness.droste.syntax.all._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import cats.instances.string._
import com.gsk.kg.engine.data.ToTree._
import DAG._
import com.gsk.kg.sparqlparser.StringVal.{GRAPH_VARIABLE, STRING}
import higherkindness.droste.data.Fix
import com.gsk.kg.engine.data.ChunkedList
import com.gsk.kg.sparqlparser.Expr
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import com.gsk.kg.engine.data.TreeRep
import com.gsk.kg.engine.scalacheck.DrosteImplicits
import com.gsk.kg.engine.scalacheck.ExpressionArbitraries
import com.gsk.kg.engine.scalacheck.DAGArbitraries
import com.gsk.kg.engine.scalacheck.ChunkedListArbitraries
import org.scalacheck.Arbitrary

class DAGSpec
    extends AnyFlatSpec
    with Matchers
    with ScalaCheckDrivenPropertyChecks
    with DrosteImplicits
    with DAGArbitraries
    with ChunkedListArbitraries {

  type T = Fix[DAG]
  val T = higherkindness.droste.Project[DAG, T]

  "DAG" should "be able to perform rewrites" in {
    val join: T = joinR(
      bgpR(
        ChunkedList(
          Expr.Quad(STRING("one"), STRING("two"), STRING("three"), GRAPH_VARIABLE)
        )
      ),
      bgpR(
        ChunkedList(
          Expr.Quad(STRING("four"), STRING("five"), STRING("six"), GRAPH_VARIABLE)
        )
      )
    )

    val joinsAsBGP: PartialFunction[DAG[T], DAG[T]] = {
      case j @ Join(l, r) =>
        (T.coalgebra(l), T.coalgebra(r)) match {
          case (BGP(tl), BGP(tr)) => bgp(tl concat tr)
          case _                  => j
        }
    }

    T.coalgebra(join).rewrite(joinsAsBGP) shouldEqual bgpR(
      ChunkedList(
        Expr.Quad(STRING("one"), STRING("two"), STRING("three"), GRAPH_VARIABLE),
        Expr.Quad(STRING("four"), STRING("five"), STRING("six"), GRAPH_VARIABLE)
      )
    )
  }
}
