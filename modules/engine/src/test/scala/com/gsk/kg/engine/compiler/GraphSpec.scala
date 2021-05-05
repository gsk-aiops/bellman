package com.gsk.kg.engine.compiler

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row

import com.gsk.kg.engine.Compiler
import com.gsk.kg.sparqlparser.TestConfig

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class GraphSpec
    extends AnyWordSpec
    with Matchers
    with DataFrameSuiteBase
    with TestConfig {

  import sqlContext.implicits._

  "perform query with GRAPH expression on default and named graphs" when {

    "simple specific graph" should {

      "execute and obtain expected results with one graph specified" in {

        val df: DataFrame = List(
          // Default graph
          (
            "http://example.org/bob",
            "http://purl.org/dc/elements/1.1/publisher",
            "Bob Hacker",
            ""
          ),
          (
            "http://example.org/alice",
            "http://purl.org/dc/elements/1.1/publisher",
            "Alice Hacker",
            ""
          ),
          // Alice graph
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/name",
            "Alice",
            "http://example.org/alice"
          ),
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/mbox",
            "mailto:alice@work.example.org",
            "http://example.org/alice"
          ),
          // Bob graph
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/name",
            "Bob",
            "http://example.org/bob"
          ),
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/mbox",
            "mailto:bob@oldcorp.example.org",
            "http://example.org/bob"
          )
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            |PREFIX dc: <http://purl.org/dc/elements/1.1/>
            |PREFIX ex: <http://example.org/>
            |
            |SELECT ?mbox
            |FROM <http://example.org/dft.ttl>
            |FROM NAMED <http://example.org/alice>
            |FROM NAMED <http://example.org/bob>
            |WHERE
            |{
            |   GRAPH ex:alice { ?x foaf:mbox ?mbox }
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 1
        result.right.get.collect.toSet shouldEqual Set(
          Row("<mailto:alice@work.example.org>")
        )
      }

      "execute and obtain expected results with one graph specified and UNION inside GRAPH statement" in {

        val df: DataFrame = List(
          // Default graph
          (
            "http://example.org/bob",
            "http://purl.org/dc/elements/1.1/publisher",
            "Bob Hacker",
            ""
          ),
          (
            "http://example.org/alice",
            "http://purl.org/dc/elements/1.1/publisher",
            "Alice Hacker",
            ""
          ),
          // Alice graph
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/name",
            "Alice",
            "http://example.org/alice"
          ),
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/mbox",
            "mailto:alice@work.example.org",
            "http://example.org/alice"
          ),
          // Bob graph
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/name",
            "Bob",
            "http://example.org/bob"
          ),
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/mbox",
            "mailto:bob@oldcorp.example.org",
            "http://example.org/bob"
          )
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            |PREFIX dc: <http://purl.org/dc/elements/1.1/>
            |PREFIX ex: <http://example.org/>
            |
            |SELECT ?mbox ?name
            |FROM <http://example.org/dft.ttl>
            |FROM NAMED <http://example.org/alice>
            |FROM NAMED <http://example.org/bob>
            |WHERE
            |{
            |   GRAPH ex:alice {
            |     { ?x foaf:mbox ?mbox }
            |     UNION
            |     { ?x foaf:name ?name }
            |   }
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 2
        result.right.get.collect.toSet shouldEqual Set(
          Row("<mailto:alice@work.example.org>", null),
          Row(null, "\"Alice\"")
        )
      }

      "execute and obtain expected results with one graph specified and JOIN inside GRAPH statement" in {

        val df: DataFrame = List(
          // Default graph
          (
            "http://example.org/bob",
            "http://purl.org/dc/elements/1.1/publisher",
            "Bob Hacker",
            ""
          ),
          (
            "http://example.org/alice",
            "http://purl.org/dc/elements/1.1/publisher",
            "Alice Hacker",
            ""
          ),
          // Alice graph
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/name",
            "Alice",
            "http://example.org/alice"
          ),
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/mbox",
            "mailto:alice@work.example.org",
            "http://example.org/alice"
          ),
          // Bob graph
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/name",
            "Bob",
            "http://example.org/bob"
          ),
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/mbox",
            "mailto:bob@oldcorp.example.org",
            "http://example.org/bob"
          )
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            |PREFIX dc: <http://purl.org/dc/elements/1.1/>
            |PREFIX ex: <http://example.org/>
            |
            |SELECT ?mbox ?name
            |FROM <http://example.org/dft.ttl>
            |FROM NAMED <http://example.org/alice>
            |FROM NAMED <http://example.org/bob>
            |WHERE
            |{
            |   GRAPH ex:alice {
            |     ?x foaf:mbox ?mbox .
            |     ?x foaf:name ?name .
            |   }
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 1
        result.right.get.collect.toSet shouldEqual Set(
          Row("<mailto:alice@work.example.org>", "\"Alice\"")
        )
      }

      "execute and obtain expected results with one graph specified and OPTIONAL inside GRAPH statement" in {}
    }

    "multiple specific named graphs" should {

      "execute and obtain expected results when UNION with common variable bindings" in {

        val df: DataFrame = List(
          // Default graph
          (
            "http://example.org/bob",
            "http://purl.org/dc/elements/1.1/publisher",
            "Bob Hacker",
            ""
          ),
          (
            "http://example.org/alice",
            "http://purl.org/dc/elements/1.1/publisher",
            "Alice Hacker",
            ""
          ),
          (
            "http://example.org/charles",
            "http://purl.org/dc/elements/1.1/publisher",
            "Charles Hacker",
            ""
          ),
          // Alice graph
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/name",
            "Alice",
            "http://example.org/alice"
          ),
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/mbox",
            "mailto:alice@work.example.org",
            "http://example.org/alice"
          ),
          // Bob graph
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/name",
            "Bob",
            "http://example.org/bob"
          ),
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/mbox",
            "mailto:bob@oldcorp.example.org",
            "http://example.org/bob"
          ),
          // Charles graph
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/name",
            "Charles",
            "http://example.org/charles"
          ),
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/mbox",
            "mailto:charles@work.example.org",
            "http://example.org/charles"
          )
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            |PREFIX dc: <http://purl.org/dc/elements/1.1/>
            |PREFIX ex: <http://example.org/>
            |
            |SELECT ?x ?y ?mbox
            |FROM <http://example.org/dft.ttl>
            |FROM NAMED <http://example.org/alice>
            |FROM NAMED <http://example.org/bob>
            |FROM NAMED <http://example.org/charles>
            |WHERE
            |{
            |   { GRAPH ex:alice { ?x foaf:mbox ?mbox } }
            |   UNION
            |   { GRAPH ex:bob { ?y foaf:mbox ?mbox } }
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 2
        result.right.get.collect.toSet shouldEqual Set(
          Row("_:a", null, "<mailto:alice@work.example.org>"),
          Row(null, "_:a", "<mailto:bob@oldcorp.example.org>")
        )
      }

      "execute and obtain expected results when JOIN with common variable bindings" in {

        val df: DataFrame = List(
          // Default graph
          (
            "http://example.org/bob",
            "http://purl.org/dc/elements/1.1/publisher",
            "Bob Hacker",
            ""
          ),
          (
            "http://example.org/alice",
            "http://purl.org/dc/elements/1.1/publisher",
            "Alice Hacker",
            ""
          ),
          // Alice graph
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/name",
            "Alice",
            "http://example.org/alice"
          ),
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/mbox",
            "mailto:alice@work.example.org",
            "http://example.org/alice"
          ),
          // Bob graph
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/name",
            "Bob",
            "http://example.org/bob"
          ),
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/mbox",
            "mailto:bob@work.example.org",
            "http://example.org/bob"
          )
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            |PREFIX dc: <http://purl.org/dc/elements/1.1/>
            |PREFIX ex: <http://example.org/>
            |
            |SELECT ?x ?mbox ?name
            |FROM <http://example.org/dft.ttl>
            |FROM NAMED <http://example.org/alice>
            |FROM NAMED <http://example.org/bob>
            |WHERE
            |{
            |   GRAPH ex:alice { ?x foaf:mbox ?mbox . }
            |   GRAPH ex:bob { ?x foaf:name ?name . }
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 1
        result.right.get.collect.toSet shouldEqual Set(
          Row("_:a", "<mailto:alice@work.example.org>", "\"Bob\"")
        )
      }

      "execute and obtain expected results when JOIN with no common variable bindings" in {

        val df: DataFrame = List(
          // Default graph
          (
            "http://example.org/bob",
            "http://purl.org/dc/elements/1.1/publisher",
            "Bob Hacker",
            ""
          ),
          (
            "http://example.org/alice",
            "http://purl.org/dc/elements/1.1/publisher",
            "Alice Hacker",
            ""
          ),
          // Alice graph
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/name",
            "Alice",
            "http://example.org/alice"
          ),
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/mbox",
            "mailto:alice@work.example.org",
            "http://example.org/alice"
          ),
          // Bob graph
          (
            "_:b",
            "http://xmlns.com/foaf/0.1/name",
            "Bob",
            "http://example.org/bob"
          ),
          (
            "_:b",
            "http://xmlns.com/foaf/0.1/mbox",
            "mailto:bob@work.example.org",
            "http://example.org/bob"
          )
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            |PREFIX dc: <http://purl.org/dc/elements/1.1/>
            |PREFIX ex: <http://example.org/>
            |
            |SELECT ?x ?y
            |FROM <http://example.org/dft.ttl>
            |FROM NAMED <http://example.org/alice>
            |FROM NAMED <http://example.org/bob>
            |WHERE
            |{
            |   GRAPH ex:alice { ?x foaf:mbox <mailto:alice@work.example.org> . }
            |   GRAPH ex:bob { ?y foaf:mbox <mailto:bob@work.example.org> . }
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 1
        result.right.get.collect.toSet shouldEqual Set(Row("_:a", "_:b"))
      }

      "execute and obtain expected results when OPTIONAL with common variable bindings" in {

        val df: DataFrame = List(
          // Default graph
          (
            "http://example.org/bob",
            "http://purl.org/dc/elements/1.1/publisher",
            "Bob Hacker",
            ""
          ),
          (
            "http://example.org/alice",
            "http://purl.org/dc/elements/1.1/publisher",
            "Alice Hacker",
            ""
          ),
          (
            "http://example.org/charles",
            "http://purl.org/dc/elements/1.1/publisher",
            "Charles Hacker",
            ""
          ),
          // Alice graph
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/name",
            "Alice",
            "http://example.org/alice"
          ),
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/mbox",
            "mailto:alice@work.example.org",
            "http://example.org/alice"
          ),
          // Bob graph
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/name",
            "Bob",
            "http://example.org/bob"
          ),
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/mbox",
            "mailto:bob@oldcorp.example.org",
            "http://example.org/bob"
          ),
          // Charles graph
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/name",
            "Charles",
            "http://example.org/charles"
          ),
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/mbox",
            "mailto:charles@work.example.org",
            "http://example.org/charles"
          )
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            |PREFIX dc: <http://purl.org/dc/elements/1.1/>
            |PREFIX ex: <http://example.org/>
            |
            |SELECT ?x ?mbox ?name
            |FROM <http://example.org/dft.ttl>
            |FROM NAMED <http://example.org/alice>
            |FROM NAMED <http://example.org/bob>
            |WHERE  {
            |  GRAPH ex:alice { ?x foaf:name ?name . }
            |  OPTIONAL {
            |    GRAPH ex:bob { ?x foaf:mbox ?mbox }
            |  }
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 1
        result.right.get.collect.toSet shouldEqual Set(
          Row("_:a", "<mailto:bob@oldcorp.example.org>", "\"Alice\"")
        )
      }

      "execute and obtain expected results when OPTIONAL with no common variable bindings" in {

        val df: DataFrame = List(
          // Default graph
          (
            "http://example.org/bob",
            "http://purl.org/dc/elements/1.1/publisher",
            "Bob Hacker",
            ""
          ),
          (
            "http://example.org/alice",
            "http://purl.org/dc/elements/1.1/publisher",
            "Alice Hacker",
            ""
          ),
          (
            "http://example.org/charles",
            "http://purl.org/dc/elements/1.1/publisher",
            "Charles Hacker",
            ""
          ),
          // Alice graph
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/name",
            "Alice",
            "http://example.org/alice"
          ),
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/mbox",
            "mailto:alice@work.example.org",
            "http://example.org/alice"
          ),
          // Bob graph
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/name",
            "Bob",
            "http://example.org/bob"
          ),
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/mbox",
            "mailto:bob@oldcorp.example.org",
            "http://example.org/bob"
          ),
          // Charles graph
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/name",
            "Charles",
            "http://example.org/charles"
          ),
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/mbox",
            "mailto:charles@work.example.org",
            "http://example.org/charles"
          )
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            |PREFIX dc: <http://purl.org/dc/elements/1.1/>
            |PREFIX ex: <http://example.org/>
            |
            |SELECT ?x ?y ?mbox ?name
            |FROM <http://example.org/dft.ttl>
            |FROM NAMED <http://example.org/alice>
            |FROM NAMED <http://example.org/bob>
            |WHERE  {
            |  GRAPH ex:alice { ?x foaf:name ?name . }
            |  OPTIONAL {
            |    GRAPH ex:bob { ?y foaf:mbox ?mbox }
            |  }
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 1
        result.right.get.collect.toSet shouldEqual Set(
          Row("_:a", "_:a", "<mailto:bob@oldcorp.example.org>", "\"Alice\"")
        )
      }
    }

    "mixing default and named graph" should {

      "execute and obtain expected results when UNION with common variable bindings" in {

        val df: DataFrame = List(
          // Default graph - Alice
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/name",
            "Alice",
            "http://example.org/alice"
          ),
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/mbox",
            "mailto:alice@work.example.org",
            "http://example.org/alice"
          ),
          // Named graph - Bob
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/name",
            "Bob",
            "http://example.org/bob"
          ),
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/mbox",
            "mailto:bob@oldcorp.example.org",
            "http://example.org/bob"
          )
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            |PREFIX ex: <http://example.org/>
            |
            |SELECT ?x ?mbox ?name
            |FROM <http://example.org/alice>
            |FROM NAMED <http://example.org/bob>
            |WHERE
            |{
            |   { ?x foaf:name ?name }
            |   UNION
            |   { GRAPH ex:bob { ?x foaf:mbox ?mbox } }
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 2
        result.right.get.collect.toSet shouldEqual Set(
          Row("_:a", null, "\"Alice\""),
          Row("_:a", "<mailto:bob@oldcorp.example.org>", null)
        )
      }

      "execute and obtain expected results when JOIN with common variable bindings" in {

        val df: DataFrame = List(
          // Default graph - Alice
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/name",
            "Alice",
            "http://example.org/alice"
          ),
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/mbox",
            "mailto:alice@work.example.org",
            "http://example.org/alice"
          ),
          // Default graph - Charles
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/name",
            "Charles",
            "http://example.org/charles"
          ),
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/mbox",
            "mailto:charles@oldcorp.example.org",
            "http://example.org/charles"
          ),
          // Named graph - Bob
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/name",
            "Bob",
            "http://example.org/bob"
          ),
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/mbox",
            "mailto:bob@oldcorp.example.org",
            "http://example.org/bob"
          )
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            |PREFIX ex: <http://example.org/>
            |
            |SELECT ?x ?mbox ?name
            |FROM <http://example.org/alice>
            |FROM <http://example.org/charles>
            |FROM NAMED <http://example.org/bob>
            |WHERE
            |{
            |   ?x foaf:name ?name .
            |   GRAPH ex:bob { ?x foaf:mbox ?mbox }
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 2
        result.right.get.collect.toSet shouldEqual Set(
          Row("_:a", "<mailto:bob@oldcorp.example.org>", "\"Alice\""),
          Row("_:a", "<mailto:bob@oldcorp.example.org>", "\"Charles\"")
        )
      }

      "execute and obtain expected results when JOIN with no common variable bindings" in {

        val df: DataFrame = List(
          // Default graph - Alice
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/name",
            "Alice",
            "http://example.org/alice"
          ),
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/mbox",
            "mailto:alice@work.example.org",
            "http://example.org/alice"
          ),
          // Default graph - Charles
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/name",
            "Charles",
            "http://example.org/charles"
          ),
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/mbox",
            "mailto:charles@oldcorp.example.org",
            "http://example.org/charles"
          ),
          // Named graph - Bob
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/name",
            "Bob",
            "http://example.org/bob"
          ),
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/mbox",
            "mailto:bob@oldcorp.example.org",
            "http://example.org/bob"
          )
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            |PREFIX ex: <http://example.org/>
            |
            |SELECT ?x ?y ?mbox ?name
            |FROM <http://example.org/alice>
            |FROM <http://example.org/charles>
            |FROM NAMED <http://example.org/bob>
            |WHERE
            |{
            |   ?x foaf:name ?name .
            |   GRAPH ex:bob { ?y foaf:mbox ?mbox }
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 2
        result.right.get.collect.toSet shouldEqual Set(
          Row("_:a", "_:a", "<mailto:bob@oldcorp.example.org>", "\"Alice\""),
          Row("_:a", "_:a", "<mailto:bob@oldcorp.example.org>", "\"Charles\"")
        )
      }

      "execute and obtain expected results when OPTIONAL with common variable bindings" in {

        val df: DataFrame = List(
          // Default graph - Alice
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/name",
            "Alice",
            "http://example.org/alice"
          ),
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/mbox",
            "mailto:alice@work.example.org",
            "http://example.org/alice"
          ),
          // Default graph - Charles
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/name",
            "Charles",
            "http://example.org/charles"
          ),
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/mbox",
            "mailto:charles@oldcorp.example.org",
            "http://example.org/charles"
          ),
          // Named graph - Bob
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/name",
            "Bob",
            "http://example.org/bob"
          ),
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/mbox",
            "mailto:bob@oldcorp.example.org",
            "http://example.org/bob"
          )
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            |PREFIX ex: <http://example.org/>
            |
            |SELECT ?x ?mbox ?name
            |FROM <http://example.org/alice>
            |FROM <http://example.org/charles>
            |FROM NAMED <http://example.org/bob>
            |WHERE
            |{
            |   ?x foaf:name ?name .
            |   OPTIONAL {
            |     GRAPH ex:bob { ?x foaf:mbox ?mbox }
            |   }
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 2
        result.right.get.collect.toSet shouldEqual Set(
          Row("_:a", "<mailto:bob@oldcorp.example.org>", "\"Alice\""),
          Row("_:a", "<mailto:bob@oldcorp.example.org>", "\"Charles\"")
        )
      }

      "execute and obtain expected results when OPTIONAL with no common variable bindings" in {

        val df: DataFrame = List(
          // Default graph - Alice
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/name",
            "Alice",
            "http://example.org/alice"
          ),
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/mbox",
            "mailto:alice@work.example.org",
            "http://example.org/alice"
          ),
          // Default graph - Charles
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/name",
            "Charles",
            "http://example.org/charles"
          ),
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/mbox",
            "mailto:charles@oldcorp.example.org",
            "http://example.org/charles"
          ),
          // Named graph - Bob
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/name",
            "Bob",
            "http://example.org/bob"
          ),
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/mbox",
            "mailto:bob@oldcorp.example.org",
            "http://example.org/bob"
          )
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            |PREFIX ex: <http://example.org/>
            |
            |SELECT ?x ?y ?mbox ?name
            |FROM <http://example.org/alice>
            |FROM <http://example.org/charles>
            |FROM NAMED <http://example.org/bob>
            |WHERE
            |{
            |   ?x foaf:name ?name .
            |   OPTIONAL {
            |     GRAPH ex:bob { ?y foaf:mbox ?mbox }
            |   }
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 2
        result.right.get.collect.toSet shouldEqual Set(
          Row("_:a", "_:a", "<mailto:bob@oldcorp.example.org>", "\"Alice\""),
          Row("_:a", "_:a", "<mailto:bob@oldcorp.example.org>", "\"Charles\"")
        )
      }
    }

    "graph variables" should {

      "execute and obtain expected results when GRAPH with variable and same variable binding on default graph BGP" in {
        import sqlContext.implicits._

        val df: DataFrame = List(
          // Default graph
          (
            "http://example.org/bob",
            "http://purl.org/dc/elements/1.1/publisher",
            "Bob Hacker",
            "http://example.org/dft.ttl"
          ),
          (
            "http://example.org/alice",
            "http://purl.org/dc/elements/1.1/publisher",
            "Alice Hacker",
            "http://example.org/dft.ttl"
          ),
          (
            "http://example.org/charles",
            "http://purl.org/dc/elements/1.1/publisher",
            "Charles Hacker",
            "http://example.org/dft.ttl"
          ),
          // Alice graph
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/name",
            "Alice",
            "http://example.org/alice"
          ),
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/mbox",
            "mailto:alice@work.example.org",
            "http://example.org/alice"
          ),
          // Bob graph
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/name",
            "Bob",
            "http://example.org/bob"
          ),
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/mbox",
            "mailto:bob@oldcorp.example.org",
            "http://example.org/bob"
          ),
          // Charles graph
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/name",
            "Charles",
            "http://example.org/charles"
          ),
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/mbox",
            "mailto:charles@work.example.org",
            "http://example.org/charles"
          )
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            |PREFIX dc: <http://purl.org/dc/elements/1.1/>
            |PREFIX ex: <http://example.org/>
            |
            |SELECT ?who ?g ?mbox
            |FROM <http://example.org/dft.ttl>
            |FROM NAMED <http://example.org/alice>
            |FROM NAMED <http://example.org/bob>
            |FROM NAMED <http://example.org/charles>
            |WHERE
            |{
            |   ?g dc:publisher ?who .
            |   GRAPH ?g { ?x foaf:mbox ?mbox }
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 3
        result.right.get.collect.toSet shouldEqual Set(
          Row(
            "\"Alice Hacker\"",
            "<http://example.org/alice>",
            "<mailto:alice@work.example.org>"
          ),
          Row(
            "\"Bob Hacker\"",
            "<http://example.org/bob>",
            "<mailto:bob@oldcorp.example.org>"
          ),
          Row(
            "\"Charles Hacker\"",
            "<http://example.org/charles>",
            "<mailto:charles@work.example.org>"
          )
        )
      }

      "execute and obtain expected results when GRAPH with variable, one named graph and common variables" in {

        val df: DataFrame = List(
          // Default graph
          (
            "http://example.org/bob",
            "http://purl.org/dc/elements/1.1/publisher",
            "Bob Hacker",
            "http://example.org/dft.ttl"
          ),
          (
            "http://example.org/alice",
            "http://purl.org/dc/elements/1.1/publisher",
            "Alice Hacker",
            "http://example.org/dft.ttl"
          ),
          (
            "http://example.org/charles",
            "http://purl.org/dc/elements/1.1/publisher",
            "Charles Hacker",
            "http://example.org/dft.ttl"
          ),
          // Alice graph
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/name",
            "Alice",
            "http://example.org/alice"
          ),
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/mbox",
            "mailto:alice@work.example.org",
            "http://example.org/alice"
          ),
          // Bob graph
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/name",
            "Bob",
            "http://example.org/bob"
          ),
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/mbox",
            "mailto:bob@oldcorp.example.org",
            "http://example.org/bob"
          ),
          // Charles graph
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/name",
            "Charles",
            "http://example.org/charles"
          ),
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/mbox",
            "mailto:charles@work.example.org",
            "http://example.org/charles"
          )
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            |PREFIX ex: <http://example.org/>
            |
            |SELECT ?x ?mbox ?name ?g
            |FROM <http://example.org/alice>
            |FROM <http://example.org/charles>
            |FROM NAMED <http://example.org/bob>
            |WHERE
            |{
            |   ?x foaf:name ?name .
            |   GRAPH ?g { ?x foaf:mbox ?mbox }
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 2
        result.right.get.collect.toSet shouldEqual Set(
          Row(
            "_:a",
            "<mailto:bob@oldcorp.example.org>",
            "\"Alice\"",
            "<http://example.org/bob>"
          ),
          Row(
            "_:a",
            "<mailto:bob@oldcorp.example.org>",
            "\"Charles\"",
            "<http://example.org/bob>"
          )
        )
      }

      "execute and obtain expected results when GRAPH with variable, two named graph and common variables" in {

        val df: DataFrame = List(
          // Default graph
          (
            "http://example.org/bob",
            "http://purl.org/dc/elements/1.1/publisher",
            "Bob Hacker",
            "http://example.org/dft.ttl"
          ),
          (
            "http://example.org/alice",
            "http://purl.org/dc/elements/1.1/publisher",
            "Alice Hacker",
            "http://example.org/dft.ttl"
          ),
          (
            "http://example.org/charles",
            "http://purl.org/dc/elements/1.1/publisher",
            "Charles Hacker",
            "http://example.org/dft.ttl"
          ),
          // Alice graph
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/name",
            "Alice",
            "http://example.org/alice"
          ),
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/mbox",
            "mailto:alice@work.example.org",
            "http://example.org/alice"
          ),
          // Bob graph
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/name",
            "Bob",
            "http://example.org/bob"
          ),
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/mbox",
            "mailto:bob@oldcorp.example.org",
            "http://example.org/bob"
          ),
          // Charles graph
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/name",
            "Charles",
            "http://example.org/charles"
          ),
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/mbox",
            "mailto:charles@work.example.org",
            "http://example.org/charles"
          )
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            |PREFIX ex: <http://example.org/>
            |
            |SELECT ?x ?mbox ?name ?g
            |FROM <http://example.org/alice>
            |FROM NAMED <http://example.org/charles>
            |FROM NAMED <http://example.org/bob>
            |WHERE
            |{
            |   ?x foaf:name ?name .
            |   GRAPH ?g { ?x foaf:mbox ?mbox }
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 2
        result.right.get.collect.toSet shouldEqual Set(
          Row(
            "_:a",
            "<mailto:charles@work.example.org>",
            "\"Alice\"",
            "<http://example.org/charles>"
          ),
          Row(
            "_:a",
            "<mailto:bob@oldcorp.example.org>",
            "\"Alice\"",
            "<http://example.org/bob>"
          )
        )
      }

      "execute and obtain expected results when GRAPH with variable, two named graph and no common variables" in {

        val df: DataFrame = List(
          // Default graph
          (
            "http://example.org/bob",
            "http://purl.org/dc/elements/1.1/publisher",
            "Bob Hacker",
            "http://example.org/dft.ttl"
          ),
          (
            "http://example.org/alice",
            "http://purl.org/dc/elements/1.1/publisher",
            "Alice Hacker",
            "http://example.org/dft.ttl"
          ),
          (
            "http://example.org/charles",
            "http://purl.org/dc/elements/1.1/publisher",
            "Charles Hacker",
            "http://example.org/dft.ttl"
          ),
          // Alice graph
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/name",
            "Alice",
            "http://example.org/alice"
          ),
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/mbox",
            "mailto:alice@work.example.org",
            "http://example.org/alice"
          ),
          // Bob graph
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/name",
            "Bob",
            "http://example.org/bob"
          ),
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/mbox",
            "mailto:bob@oldcorp.example.org",
            "http://example.org/bob"
          ),
          // Charles graph
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/name",
            "Charles",
            "http://example.org/charles"
          ),
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/mbox",
            "mailto:charles@work.example.org",
            "http://example.org/charles"
          )
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            |PREFIX ex: <http://example.org/>
            |
            |SELECT ?x ?y ?mbox ?name ?g
            |FROM <http://example.org/alice>
            |FROM NAMED <http://example.org/charles>
            |FROM NAMED <http://example.org/bob>
            |WHERE
            |{
            |   ?x foaf:name ?name .
            |   GRAPH ?g { ?y foaf:mbox ?mbox }
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 2
        result.right.get.collect.toSet shouldEqual Set(
          Row(
            "_:a",
            "_:a",
            "<mailto:charles@work.example.org>",
            "\"Alice\"",
            "<http://example.org/charles>"
          ),
          Row(
            "_:a",
            "_:a",
            "<mailto:bob@oldcorp.example.org>",
            "\"Alice\"",
            "<http://example.org/bob>"
          )
        )
      }

      "execute and obtain expected results when GRAPH with variable over mixed named and default graphs with JOIN" in {

        import sqlContext.implicits._

        val df: DataFrame = List(
          // Default graph
          (
            "http://example.org/bob",
            "http://purl.org/dc/elements/1.1/publisher",
            "Bob Hacker",
            "http://example.org/dft.ttl"
          ),
          (
            "http://example.org/alice",
            "http://purl.org/dc/elements/1.1/publisher",
            "Alice Hacker",
            "http://example.org/dft.ttl"
          ),
          (
            "http://example.org/charles",
            "http://purl.org/dc/elements/1.1/publisher",
            "Charles Hacker",
            "http://example.org/dft.ttl"
          ),
          // Alice graph
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/name",
            "Alice",
            "http://example.org/alice"
          ),
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/mbox",
            "mailto:alice@work.example.org",
            "http://example.org/alice"
          ),
          // Bob graph
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/name",
            "Bob",
            "http://example.org/bob"
          ),
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/mbox",
            "mailto:bob@oldcorp.example.org",
            "http://example.org/bob"
          ),
          // Charles graph
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/name",
            "Charles",
            "http://example.org/charles"
          ),
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/mbox",
            "mailto:charles@work.example.org",
            "http://example.org/charles"
          )
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            |PREFIX ex: <http://example.org/>
            |
            |SELECT ?x ?mbox ?name ?g
            |FROM <http://example.org/alice>
            |FROM <http://example.org/charles>
            |FROM NAMED <http://example.org/bob>
            |WHERE
            |{
            |   GRAPH ?g {
            |     ?x foaf:name ?name .
            |     GRAPH ex:bob { ?x foaf:mbox ?mbox }
            |   }
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 1
        result.right.get.collect.toSet shouldEqual Set(
          Row(
            "_:a",
            "<mailto:bob@oldcorp.example.org>",
            "\"Bob\"",
            "<http://example.org/bob>"
          )
        )
      }
    }

    "case of study graph queries" should {

      // TODO: This query seems not to be consistent with Jena, shall be analyzed as a case of study
      "execute and obtain expected results when GRAPH with variable over mixed named and default graphs with UNION" in {

        import sqlContext.implicits._

        val df: DataFrame = List(
          // Default graph
          (
            "http://example.org/bob",
            "http://purl.org/dc/elements/1.1/publisher",
            "Bob Hacker",
            "http://example.org/dft.ttl"
          ),
          (
            "http://example.org/alice",
            "http://purl.org/dc/elements/1.1/publisher",
            "Alice Hacker",
            "http://example.org/dft.ttl"
          ),
          (
            "http://example.org/charles",
            "http://purl.org/dc/elements/1.1/publisher",
            "Charles Hacker",
            "http://example.org/dft.ttl"
          ),
          // Alice graph
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/name",
            "Alice",
            "http://example.org/alice"
          ),
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/mbox",
            "mailto:alice@work.example.org",
            "http://example.org/alice"
          ),
          // Bob graph
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/name",
            "Bob",
            "http://example.org/bob"
          ),
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/mbox",
            "mailto:bob@oldcorp.example.org",
            "http://example.org/bob"
          ),
          // Charles graph
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/name",
            "Charles",
            "http://example.org/charles"
          ),
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/mbox",
            "mailto:charles@work.example.org",
            "http://example.org/charles"
          )
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            |PREFIX ex: <http://example.org/>
            |
            |SELECT ?x ?mbox ?name ?g
            |FROM <http://example.org/alice>
            |FROM NAMED <http://example.org/bob>
            |FROM NAMED <http://example.org/charles>
            |WHERE
            |{
            | GRAPH ?g {
            |   { ?x foaf:name ?name . }
            |   UNION
            |   { GRAPH ex:bob { ?x foaf:mbox ?mbox } }
            | }
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 3
        result.right.get.collect.toSet shouldEqual Set(
          Row(
            "_:a",
            null,
            "\"Charles\"",
            "<http://example.org/charles>"
          ),
          Row(
            "_:a",
            null,
            "\"Bob\"",
            "<http://example.org/bob>"
          ),
          Row(
            "_:a",
            "<mailto:bob@oldcorp.example.org>",
            null,
            "<http://example.org/bob>"
          )
        )
      }

      // TODO: This query seems not to be consistent with Jena, shall be analyzed as a case of study
      "execute and obtain expected results when GRAPH over mixed named and default graphs with UNION" in {

        import sqlContext.implicits._

        val df: DataFrame = List(
          // Default graph
          (
            "http://example.org/bob",
            "http://purl.org/dc/elements/1.1/publisher",
            "Bob Hacker",
            "http://example.org/dft.ttl"
          ),
          (
            "http://example.org/alice",
            "http://purl.org/dc/elements/1.1/publisher",
            "Alice Hacker",
            "http://example.org/dft.ttl"
          ),
          (
            "http://example.org/charles",
            "http://purl.org/dc/elements/1.1/publisher",
            "Charles Hacker",
            "http://example.org/dft.ttl"
          ),
          // Alice graph
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/name",
            "Alice",
            "http://example.org/alice"
          ),
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/mbox",
            "mailto:alice@work.example.org",
            "http://example.org/alice"
          ),
          // Bob graph
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/name",
            "Bob",
            "http://example.org/bob"
          ),
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/mbox",
            "mailto:bob@oldcorp.example.org",
            "http://example.org/bob"
          ),
          // Charles graph
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/name",
            "Charles",
            "http://example.org/charles"
          ),
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/mbox",
            "mailto:charles@work.example.org",
            "http://example.org/charles"
          )
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            |PREFIX ex: <http://example.org/>
            |
            |SELECT ?x ?mbox ?name
            |FROM <http://example.org/alice>
            |FROM NAMED <http://example.org/bob>
            |FROM NAMED <http://example.org/charles>
            |WHERE
            |{
            |  {
            |    GRAPH ex:bob {
            |     { ?x foaf:name ?name . }
            |     UNION
            |     { GRAPH ex:bob { ?x foaf:mbox ?mbox } }
            |    }
            |  }
            |  UNION
            |  {
            |    GRAPH ex:charles {
            |      { ?x foaf:name ?name . }
            |      UNION
            |      { GRAPH ex:bob { ?x foaf:mbox ?mbox } }
            |    }
            |  }
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 4
        result.right.get.collect.toSet shouldEqual Set(
          Row(
            "_:a",
            null,
            "\"Charles\""
          ),
          Row(
            "_:a",
            "<mailto:bob@oldcorp.example.org>",
            null
          ),
          Row(
            "_:a",
            null,
            "\"Bob\""
          ),
          Row(
            "_:a",
            "<mailto:bob@oldcorp.example.org>",
            null
          )
        )
      }

      // TODO: This query seems not to be consistent with Jena, shall be analyzed as a case of study
      "execute and obtain expected results when GRAPH with variable over mixed named and default graphs with JOIN" in {

        import sqlContext.implicits._

        val df: DataFrame = List(
          // Default graph
          (
            "http://example.org/bob",
            "http://purl.org/dc/elements/1.1/publisher",
            "Bob Hacker",
            "http://example.org/dft.ttl"
          ),
          (
            "http://example.org/alice",
            "http://purl.org/dc/elements/1.1/publisher",
            "Alice Hacker",
            "http://example.org/dft.ttl"
          ),
          (
            "http://example.org/charles",
            "http://purl.org/dc/elements/1.1/publisher",
            "Charles Hacker",
            "http://example.org/dft.ttl"
          ),
          // Alice graph
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/name",
            "Alice",
            "http://example.org/alice"
          ),
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/mbox",
            "mailto:alice@work.example.org",
            "http://example.org/alice"
          ),
          // Bob graph
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/name",
            "Bob",
            "http://example.org/bob"
          ),
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/mbox",
            "mailto:bob@oldcorp.example.org",
            "http://example.org/bob"
          ),
          // Charles graph
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/name",
            "Charles",
            "http://example.org/charles"
          ),
          (
            "_:a",
            "http://xmlns.com/foaf/0.1/mbox",
            "mailto:charles@work.example.org",
            "http://example.org/charles"
          )
        ).toDF("s", "p", "o", "g")

        val query =
          """
            |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            |PREFIX ex: <http://example.org/>
            |
            |SELECT ?x ?mbox ?name
            |FROM <http://example.org/alice>
            |FROM NAMED <http://example.org/bob>
            |FROM NAMED <http://example.org/charles>
            |WHERE
            |{
            |  GRAPH ex:bob {
            |   { ?x foaf:name ?name . }
            |   UNION
            |   { GRAPH ex:bob { ?x foaf:mbox ?mbox } }
            |  } .
            |  GRAPH ex:charles {
            |    { ?x foaf:name ?name . }
            |    UNION
            |    { GRAPH ex:bob { ?x foaf:mbox ?mbox } }
            |  }
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 0
        result.right.get.collect.toSet shouldEqual Set()
      }
    }
  }
}
