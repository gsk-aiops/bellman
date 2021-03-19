package com.gsk.kg.engine.analyzer

import cats.implicits._
import com.gsk.kg.engine.data.ToTree._

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import com.gsk.kg.sparql.syntax.all._
import higherkindness.droste.data.Fix
import com.gsk.kg.engine.DAG
import com.gsk.kg.engine.EngineError
import cats.data.NonEmptyChain

class AnalyzerSpec extends AnyFlatSpec with Matchers {

  "Analyzer.findUnboundVariables" should "find unbound variables in CONSTRUCT queries" in {
    val query = sparql"""
      CONSTRUCT {
        ?notBound <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?other
      } WHERE {
        ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?o
      }
      """

    val dag = DAG.fromQuery.apply(query)

    val result = Analyzer.analyze.apply(dag).runA(null)

    result shouldEqual Left(
      EngineError.AnalyzerError(
        NonEmptyChain(
          "found free variables VARIABLE(?notBound), VARIABLE(?other)"
        )
      )
    )
  }

  it should "find unbound variables in SELECT queries" in {
    val query = sparql"""
      SELECT ?species_node WHERE {
      <http://purl.obolibrary.org/obo/CLO_0037232> <http://www.w3.org/2000/01/rdf-schema#subClassOf> ?derived_node .
      }
      """

    val dag = DAG.fromQuery.apply(query)

    val result = Analyzer.analyze.apply(dag).runA(null)

    result shouldEqual Left(
      EngineError.AnalyzerError(
        NonEmptyChain(
          "found free variables VARIABLE(?species_node)"
        )
      )
    )
  }

}
