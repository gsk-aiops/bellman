package com.gsk.kg.engine.compiler

import org.apache.spark.sql.Row

import com.gsk.kg.engine.Compiler
import com.gsk.kg.sparqlparser.TestConfig

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
          ),
          (
            "<http://example.org/book3>",
            "<http://www.w3.org/2000/01/rdf-schema#label2>",
            "Another title"
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

        result.right.get.collect.toSet shouldEqual Set(
          Row("<http://example.org/book1>", "\"SPARQL Tutorial\""),
          Row("<http://example.org/book2>", "\"From Earth To The Moon\"")
        )
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

      // TODO: Un-ignore test when implemented support on property paths

      "inverse ^ property path" ignore {

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

      "reverse ^ property path" ignore {

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
            | ?o foaf:knows/^foaf:mbox ?s .
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result.right.get.collect.toSet shouldEqual Set()
      }

      "arbitrary length + property path" in {

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
            |SELECT ?s ?o
            |WHERE {
            | ?s foaf:knows+ ?o .
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result.right.get.collect.toSet shouldEqual Set(
          Row("<http://example.org/Alice>", "<http://example.org/Bob>"),
          Row("<http://example.org/Alice>", "<http://example.org/Charles>"),
          Row("<http://example.org/Bob>", "<http://example.org/Charles>")
        )
      }

      "arbitrary length * property path" ignore {

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

      "optional ? property path" ignore {

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

      "negated ! property path" ignore {

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

      "fixed length {n,m} property path" ignore {

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

      "fixed length {n,} property path" ignore {

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
            | ?s foaf:knows{1,} ?o .
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result.right.get.collect.toSet shouldEqual Set()
      }

      "fixed length {,n} property path" ignore {

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
            | ?s foaf:knows{,1} ?o .
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result.right.get.collect.toSet shouldEqual Set()
      }

      "fixed length {n} property path" ignore {

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
            | ?s foaf:knows{1} ?o .
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result.right.get.collect.toSet shouldEqual Set()
      }
    }

    "perform on complex queries" when {

      "complex query 1" ignore {

        val df = List(
          (
            "<http://example.org/1>",
            "<http://example.org/a>",
            "<http://example.org/2>"
          ),
          (
            "<http://example.org/2>",
            "<http://example.org/b>",
            "<http://example.org/3>"
          ),
          (
            "<http://example.org/3>",
            "<http://example.org/c>",
            "<http://example.org/4>"
          )
        ).toDF("s", "p", "o")

        val query =
          """
            |PREFIX ex: <http://example.org/>
            |
            |SELECT ?s ?o
            |WHERE {
            | ?s ^!^ex:a*/ex:b?|ex:c+ ?o .
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result.right.get.collect.toSet shouldEqual Set()
      }

      "complex query 2" ignore {

        val df = List(
          (
            "<http://example.org/1>",
            "<http://example.org/a>",
            "<http://example.org/2>"
          ),
          (
            "<http://example.org/2>",
            "<http://example.org/b>",
            "<http://example.org/3>"
          ),
          (
            "<http://example.org/3>",
            "<http://example.org/c>",
            "<http://example.org/4>"
          )
        ).toDF("s", "p", "o")

        val query =
          """
            |PREFIX ex: <http://example.org/>
            |
            |SELECT ?s ?o
            |WHERE {
            | ?s ex:a{1}|ex:b{1,3}|ex:c{2,} ?o .
            | ?s ^!^ex:a*/ex:b?|ex:c+ ?o .
            | ?s ex:a ?o .
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result.right.get.collect.toSet shouldEqual Set()
      }
    }
  }

}
