package com.gsk.kg.engine.analyzer

import cats.data.NonEmptyChain
import cats.implicits._

import higherkindness.droste.syntax.all._

import com.gsk.kg.engine.DAG
import com.gsk.kg.engine.EngineError
import com.gsk.kg.sparqlparser.StringVal.VARIABLE
import com.gsk.kg.sparqlparser.TestConfig
import com.gsk.kg.sparqlparser.TestUtils

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AnalyzerSpec
    extends AnyFlatSpec
    with Matchers
    with TestUtils
    with TestConfig {

  "Analyzer.findUnboundVariables" should "find unbound variables in CONSTRUCT queries" in {
    val q =
      """
        |CONSTRUCT {
        | ?notBound <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?other
        |} WHERE {
        | ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?o
        |}
        |""".stripMargin
    val (query, _) = parse(q, config)

    val dag = DAG.fromQuery.apply(query)

    val result = Analyzer.analyze.apply(dag).runA(config, null)

    result shouldEqual Left(
      EngineError.AnalyzerError(
        NonEmptyChain(
          "found free variables VARIABLE(?notBound), VARIABLE(?other)"
        )
      )
    )
  }

  it should "find unbound variables in SELECT queries" in {
    val q =
      """
        |SELECT ?species_node WHERE {
        | <http://purl.obolibrary.org/obo/CLO_0037232> <http://www.w3.org/2000/01/rdf-schema#subClassOf> ?derived_node .
        |}
        |""".stripMargin
    val (query, _) = parse(q, config)

    val dag = DAG.fromQuery.apply(query)

    val result = Analyzer.analyze.apply(dag).runA(config, null)

    result shouldEqual Left(
      EngineError.AnalyzerError(
        NonEmptyChain(
          "found free variables VARIABLE(?species_node)"
        )
      )
    )
  }

  it should "find bound variables even when they're bound as part of expressions" in {
    val q =
      """
        |PREFIX foaf:   <http://xmlns.com/foaf/0.1/>
        |
        |SELECT   ?lit ?lit2
        |WHERE    {
        | ?x foaf:lit ?lit .
        | BIND(REPLACE(?lit, "b", "Z") AS ?lit2)
        |}
        |""".stripMargin
    val (query, _) = parse(q, config)

    val dag = DAG.fromQuery.apply(query)

    val variablesBoundInBind = dag
      .collect[List[VARIABLE], VARIABLE] { case DAG.Bind(variable, _, _) =>
        variable
      }
      .toSet

    val result = Analyzer.analyze.apply(dag).runA(config, null)

    result shouldBe a[Right[_, _]]

  }

}
