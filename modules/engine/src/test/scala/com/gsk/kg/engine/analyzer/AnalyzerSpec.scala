package com.gsk.kg.engine.analyzer

import cats.data.NonEmptyChain
import cats.implicits._

import com.gsk.kg.Graphs
import com.gsk.kg.engine.DAG
import com.gsk.kg.sparqlparser.EngineError
import com.gsk.kg.sparqlparser.Query
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

    val parsed: Either[EngineError, (Query, Graphs)] = parse(q, config)
    parsed
      .flatMap { case (query, _) =>
        val dag = DAG.fromQuery.apply(query)
        Analyzer.analyze.apply(dag).runA(config, null)
      }
      .fold(
        { error =>
          error shouldEqual EngineError.AnalyzerError(
            NonEmptyChain(
              "found free variables VARIABLE(?notBound), VARIABLE(?other)"
            )
          )
        },
        _ => fail
      )
  }

  it should "find unbound variables in SELECT queries" in {
    val q =
      """
        |SELECT ?species_node WHERE {
        | <http://purl.obolibrary.org/obo/CLO_0037232> <http://www.w3.org/2000/01/rdf-schema#subClassOf> ?derived_node .
        |}
        |""".stripMargin

    val parsed: Either[EngineError, (Query, Graphs)] = parse(q, config)
    parsed
      .flatMap { case (query, _) =>
        val dag = DAG.fromQuery.apply(query)
        Analyzer.analyze.apply(dag).runA(config, null)
      }
      .fold(
        { error =>
          error shouldEqual EngineError.AnalyzerError(
            NonEmptyChain(
              "found free variables VARIABLE(?species_node)"
            )
          )
        },
        _ => fail
      )
  }

  it should "find unbound variables when variable in ORDER BY not declared in WHERE clause" in {
    val q =
      """
        |PREFIX foaf:    <http://xmlns.com/foaf/0.1/>
        |
        |SELECT ?name
        |WHERE { ?x foaf:name ?name ; foaf:age ?age }
        |ORDER BY DESC(?name) ?age DESC(?age) ASC(?names) DESC((isBlank(?x) || isBlank(?age)))
        |""".stripMargin

    val parsed: Either[EngineError, (Query, Graphs)] = parse(q, config)
    parsed
      .flatMap { case (query, _) =>
        val dag = DAG.fromQuery.apply(query)
        Analyzer.analyze.apply(dag).runA(config, null)
      }
      .fold(
        { error =>
          error shouldEqual EngineError.AnalyzerError(
            NonEmptyChain(
              "found free variables VARIABLE(?names)"
            )
          )
        },
        _ => fail
      )
  }

  it should "find unbound variables when variable in GROUP BY not declared in WHERE clause" in {
    val q =
      """
        |SELECT ?c SUM(?b)
        |WHERE {
        | ?a ?b <http://uri.com/object>
        |} GROUP BY ?c
        |""".stripMargin

    val parsed: Either[EngineError, (Query, Graphs)] = parse(q, config)
    parsed
      .flatMap { case (query, _) =>
        val dag = DAG.fromQuery.apply(query)
        Analyzer.analyze.apply(dag).runA(config, null)
      }
      .fold(
        { error =>
          error shouldEqual EngineError.AnalyzerError(
            NonEmptyChain(
              "found free variables VARIABLE(?c)"
            )
          )
        },
        _ => fail
      )
  }

  it should "find unbound variables when variable in FILTER and not declared in BGPs" in {
    val q =
      """
        PREFIX foaf:    <http://xmlns.com/foaf/0.1/>
        |
        |SELECT  ?x ?name
        |WHERE   {
        |   ?x foaf:name ?name .
        |   FILTER(isBlank(?x) && !isBlank(?age))
        |}
        |""".stripMargin

    val parsed: Either[EngineError, (Query, Graphs)] = parse(q, config)
    parsed
      .flatMap { case (query, _) =>
        val dag = DAG.fromQuery.apply(query)
        Analyzer.analyze.apply(dag).runA(config, null)
      }
      .fold(
        { error =>
          error shouldEqual EngineError.AnalyzerError(
            NonEmptyChain(
              "found free variables VARIABLE(?age)"
            )
          )
        },
        _ => fail
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

    val parsed: Either[EngineError, (Query, Graphs)] = parse(q, config)

    parsed
      .flatMap { case (query, _) =>
        val dag = DAG.fromQuery.apply(query)
        Analyzer.analyze.apply(dag).runA(config, null)
      }
      .fold(_ => fail, _ => succeed)
  }

}
