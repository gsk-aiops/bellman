package com.gsk.kg.engine
package optimizer

import higherkindness.droste.data.Fix

import com.gsk.kg.engine.DAG._
import com.gsk.kg.engine.data.ChunkedList
import com.gsk.kg.engine.scalacheck.ChunkedListArbitraries
import com.gsk.kg.engine.scalacheck.DAGArbitraries
import com.gsk.kg.engine.scalacheck.DrosteImplicits
import com.gsk.kg.sparql.syntax.all._
import com.gsk.kg.sparqlparser.Expr
import com.gsk.kg.sparqlparser.StringVal.GRAPH_VARIABLE
import com.gsk.kg.sparqlparser.StringVal.STRING

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class ReorderBgpsSpec
    extends AnyFlatSpec
    with Matchers
    with ScalaCheckDrivenPropertyChecks
    with DrosteImplicits
    with DAGArbitraries
    with ChunkedListArbitraries {

  "ReorderBgps" should "reorder" in {

    val query = sparql"""
      prefix : <http://example.org>

      select * where {
        ?qwer :knows ?asdf .

        ?a :knows :alice .
        ?b :knows :alice .
        ?a :knows ?b .

        ?qwer :knows ?zxcv .
        ?asdf :knows ?zxcv .
      }
      """

    val dag = DAG.fromQuery.apply(query)

    ReorderBgps.reorderBgps(dag)

  }

}
