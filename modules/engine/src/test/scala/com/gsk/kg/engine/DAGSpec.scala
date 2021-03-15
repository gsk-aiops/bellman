package com.gsk.kg.engine

import higherkindness.droste.syntax.project._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import DAG._
import com.gsk.kg.sparqlparser.StringVal.STRING
import higherkindness.droste.data.Fix
import com.gsk.kg.engine.data.ChunkedList

class DAGSpec extends AnyFlatSpec with Matchers {

  type T = Fix[DAG]
  val T = higherkindness.droste.Project[DAG, T]

  "DAG" should "be able to perform rewrites" in {
    val join: T = joinR(
      bgpR(
        ChunkedList(
          tripleR(STRING("one"), STRING("two"), STRING("three"))
        )
      ),
      bgpR(
        ChunkedList(
          tripleR(STRING("four"), STRING("five"), STRING("six"))
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
        tripleR(STRING("one"), STRING("two"), STRING("three")),
        tripleR(STRING("four"), STRING("five"), STRING("six"))
      )
    )
  }

}
