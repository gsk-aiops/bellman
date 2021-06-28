package com.gsk.kg.engine.compiler

import com.gsk.kg.engine.Compiler
import com.gsk.kg.sparqlparser.TestConfig
import org.apache.spark.sql.Row
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class PropertyPathsSpec
    extends AnyWordSpec
    with Matchers
    with SparkSpec
    with TestConfig {

  import sqlContext.implicits._

  "Property Paths" should {

    "perform on simple queries" when {

      "alternative | property path" in {

        val df = List(
          (
            "<http://example.org/book1>",
            "<http://purl.org/dc/elements/1.1/title>",
            "SPARQL Tutorial"
          ),
          (
            "<http://example.org/book2>",
            "<http://www.w3.org/2000/01/rdf-schema#label>",
            "From Earth To The Moon"
          )
        ).toDF("s", "p", "o")

        val query =
          """
            |PREFIX dc: <http://purl.org/dc/elements/1.1/>
            |PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            |
            |SELECT ?book ?displayString
            |WHERE {
            | ?book dc:title|rdfs:label ?displayString .
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result.right.get.collect.toSet shouldEqual Set()
      }

      "sequence / property path" in {

        val df = List(
          (
            "<http://example.org/Alice>",
            "<http://xmlns.org/foaf/0.1/knows>",
            "<http://example.org/Bob>"
          ),
          (
            "<http://example.org/Bob>",
            "<http://xmlns.org/foaf/0.1/knows>",
            "<http://example.org/Charles>"
          ),
          (
            "<http://example.org/Charles>",
            "<http://xmlns.org/foaf/0.1/name>",
            "\"Charles\""
          )
        ).toDF("s", "p", "o")

        val query =
          """
            |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
            |
            |SELECT ?o
            |WHERE {
            | ?s foaf:knows/foaf:knows/foaf:name ?o .
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result.right.get.collect.toSet shouldEqual Set(
          Row("\"Charles\"")
        )
      }

      "reverse ^ property path" in {

        val df = List(
          (
            "<http://example.org/alice>",
            "<http://xmlns.org/foaf/0.1/mbox>",
            "<mailto:alice@example.org>"
          )
        ).toDF("s", "p", "o")

        val query =
          """
            |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
            |
            |SELECT ?o ?s
            |WHERE {
            | ?o ^foaf:mbox ?s .
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result.right.get.collect.toSet shouldEqual Set()
      }

      "arbitrary length + property path" in {

        val df = List(
          (
            "<http://example.org/alice>",
            "<http://xmlns.org/foaf/0.1/knows>",
            "<http://example.org/bob>"
          ),
          (
            "<http://example.org/bob>",
            "<http://xmlns.org/foaf/0.1/knows>",
            "<http://example.org/charles>"
          )
        ).toDF("s", "p", "o")

        val query =
          """
            |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
            |
            |SELECT ?s ?o
            |WHERE {
            | ?s foaf:knows+ ?o .
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result.right.get.collect.toSet shouldEqual Set()
      }

      "arbitrary length * property path" in {

        val df = List(
          (
            "<http://example.org/alice>",
            "<http://xmlns.org/foaf/0.1/knows>",
            "<http://example.org/bob>"
          ),
          (
            "<http://example.org/bob>",
            "<http://xmlns.org/foaf/0.1/knows>",
            "<http://example.org/charles>"
          )
        ).toDF("s", "p", "o")

        val query =
          """
            |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
            |
            |SELECT ?s ?o
            |WHERE {
            | ?s foaf:knows* ?o .
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result.right.get.collect.toSet shouldEqual Set()
      }

      "optional ? property path" in {

        val df = List(
          (
            "<http://example.org/alice>",
            "<http://xmlns.org/foaf/0.1/knows>",
            "<http://example.org/bob>"
          ),
          (
            "<http://example.org/bob>",
            "<http://xmlns.org/foaf/0.1/knows>",
            "<http://example.org/charles>"
          )
        ).toDF("s", "p", "o")

        val query =
          """
            |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
            |
            |SELECT ?s ?o
            |WHERE {
            | ?s foaf:knows? ?o .
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result.right.get.collect.toSet shouldEqual Set()
      }

      "fixed length {n,m} property path" in {

        val df = List(
          (
            "<http://example.org/alice>",
            "<http://xmlns.org/foaf/0.1/knows>",
            "<http://example.org/bob>"
          ),
          (
            "<http://example.org/bob>",
            "<http://xmlns.org/foaf/0.1/knows>",
            "<http://example.org/charles>"
          )
        ).toDF("s", "p", "o")

        val query =
          """
            |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
            |
            |SELECT ?s ?o
            |WHERE {
            | ?s foaf:knows{1, 2} ?o .
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result.right.get.collect.toSet shouldEqual Set()
      }

      "negated ! property path" in {

        val df = List(
          (
            "<http://example.org/a>",
            "<http://xmlns.org/foaf/0.1/name>",
            "\"Alice\""
          )
        ).toDF("s", "p", "o")

        val query =
          """
            |PREFIX foaf: <http://xmlns.org/foaf/0.1/>
            |
            |SELECT ?s ?o
            |WHERE {
            | ?s !(foaf:name) ?o .
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result.right.get.collect.toSet shouldEqual Set()
      }
    }

    "perform on complex queries" when {}
  }

}
