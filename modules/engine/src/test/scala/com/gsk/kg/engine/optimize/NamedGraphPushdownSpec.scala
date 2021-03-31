package com.gsk.kg.engine
package optimizer
import higherkindness.droste.data.Fix

import com.gsk.kg.engine.DAG
import com.gsk.kg.engine.DAG.BGP
import com.gsk.kg.engine.DAG.Project
import com.gsk.kg.engine.DAG.Scan
import com.gsk.kg.engine.data.ToTree
import com.gsk.kg.sparql.syntax.all.SparqlQueryInterpolator
import com.gsk.kg.sparqlparser.StringVal.GRAPH_VARIABLE

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class NamedGraphPushdownSpec
    extends AnyFlatSpec
    with Matchers
    with ScalaCheckDrivenPropertyChecks {

  type T = Fix[DAG]

  "NamedGraphPushdown" should "rename the graph column of quads when inside a GRAPH statement" in {

    val query =
      sparql"""
        PREFIX foaf: <http://xmlns.com/foaf/0.1/>
        PREFIX dc: <http://purl.org/dc/elements/1.1/>
        PREFIX ex: <http://example.org/>

        SELECT ?mbox ?name
        FROM <http://example.org/dft.ttl>
        FROM NAMED <http://example.org/alice>
        FROM NAMED <http://example.org/bob>
        WHERE
        {
           GRAPH ex:alice {
             ?x foaf:mbox ?mbox .
             ?x foaf:name ?name .
           }
        }
      """

    val dag: T = DAG.fromQuery.apply(query)
    Fix.un(dag) match {
      case Project(_, Fix(Project(_, Fix(Scan(_, Fix(BGP(quads))))))) =>
        quads.mapChunks(_.map(_.g shouldEqual GRAPH_VARIABLE))
      case _ => fail
    }

    val renamed = NamedGraphPushdown[T].apply(dag)
    Fix.un(renamed) match {
      case Project(_, Fix(Project(_, Fix(Fix(BGP(quads)))))) =>
        quads.mapChunks(_.map(_.g shouldNot be(GRAPH_VARIABLE)))
    }
  }
}
