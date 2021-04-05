package com.gsk.kg.engine.optimize

import higherkindness.droste.data.Fix

import com.gsk.kg.engine.DAG
import com.gsk.kg.engine.DAG._
import com.gsk.kg.engine.data.ChunkedList
import com.gsk.kg.sparql.syntax.all.SparqlQueryInterpolator
import com.gsk.kg.sparqlparser.Expr
import com.gsk.kg.sparqlparser.StringVal.GRAPH_VARIABLE

import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class DefaultGraphsPushdownSpec extends AnyWordSpec with Matchers {

  type T = Fix[DAG]

  val assertForAllQuads: ChunkedList[Expr.Quad] => (
      Expr.Quad => Assertion
  ) => Unit = {
    chunkedList: ChunkedList[Expr.Quad] => assert: (Expr.Quad => Assertion) =>
      chunkedList.mapChunks(_.map(assert))
  }

  "DefaultGraphsPushdown" should {

    "rename graph column of quads to contain a list of default graphs" when {

      "we have multiple default graphs" in {

        val (query, defaultGraphs) =
          sparql"""
            PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            PREFIX dc: <http://purl.org/dc/elements/1.1/>
            PREFIX ex: <http://example.org/>
    
            SELECT ?mbox ?name
            FROM <http://example.org/alice>
            FROM <http://example.org/bob>
            WHERE
            {
              ?x foaf:mbox ?mbox .
              ?x foaf:name ?name .
            }
          """

        val dag: T = DAG.fromQuery.apply(query)
        Fix.un(dag) match {
          case Project(_, Project(_, BGP(quads))) =>
            assertForAllQuads(quads)(_.g shouldEqual GRAPH_VARIABLE :: Nil)
          case _ => fail
        }

        val pushedDown = DefaultGraphsPushdown[T].apply(dag, defaultGraphs)
        Fix.un(pushedDown) match {
          case Project(_, Project(_, BGP(quads))) =>
            assertForAllQuads(quads)(_.g shouldEqual defaultGraphs)
          case _ => fail
        }
      }
    }
  }
}
