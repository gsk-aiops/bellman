package com.gsk.kg.engine.compiler

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row

import com.gsk.kg.engine.Compiler
import com.gsk.kg.sparqlparser.TestConfig

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ExistsSpec
    extends AnyWordSpec
    with Matchers
    with SparkSpec
    with TestConfig {

  import sqlContext.implicits._

  "perform EXISTS query" should {

    "execute and obtain expected results" when {

      "simple query with EXISTS" in {

        val df: DataFrame = List(
          (
            "<http://example.org/alice>",
            "<http://xmlns.com/foaf/0.1/name>",
            "Alice"
          ),
          (
            "<http://example.org/alice>",
            "<http://xmlns.com/foaf/0.1/age>",
            "23"
          ),
          (
            "<http://example.org/bob>",
            "<http://xmlns.com/foaf/0.1/name>",
            "Bob"
          ),
          (
            "<http://example.org/bob>",
            "<http://xmlns.com/foaf/0.1/age>",
            "35"
          ),
          (
            "<http://example.org/bob>",
            "<http://xmlns.com/foaf/0.1/mail>",
            "<mailto:bob@work.example.org>"
          )
        ).toDF("s", "p", "o")

        val query =
          """
            |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            |
            |SELECT ?name ?age
            |WHERE {
            |  ?s foaf:name ?name ;
            |     foaf:age ?age .
            |  EXISTS { ?s foaf:mail ?mail }
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 1
        result.right.get.collect.toSet shouldEqual Set(
          Row("\"Bob\"", "35")
        )
      }

      "combined with FILTER" in {

        val df = List(
          (
            "<http://example/alice>",
            "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
            "<http://xmlns.com/foaf/0.1/Person>"
          ),
          (
            "<http://example/alice>",
            "<http://xmlns.com/foaf/0.1/name>",
            "Alice"
          ),
          (
            "<http://example/bob>",
            "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
            "<http://xmlns.com/foaf/0.1/Person>"
          )
        ).toDF("s", "p", "o")

        val query =
          """
            |PREFIX  rdf:    <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            |PREFIX  foaf:   <http://xmlns.com/foaf/0.1/>
            |
            |SELECT ?person
            |WHERE
            |{
            |    ?person rdf:type  foaf:Person .
            |    FILTER EXISTS { ?person foaf:name ?name }
            |}
          |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 1
        result.right.get.collect.toSet shouldEqual Set(
          Row("<http://example/alice>")
        )
      }
    }
  }

  "perform NOT EXISTS query" should {

    "execute and obtain expected results" when {

      "simple query with NOT EXISTS" in {

        val df: DataFrame = List(
          (
            "<http://example.org/alice>",
            "<http://xmlns.com/foaf/0.1/name>",
            "Alice"
          ),
          (
            "<http://example.org/alice>",
            "<http://xmlns.com/foaf/0.1/age>",
            "23"
          ),
          (
            "<http://example.org/bob>",
            "<http://xmlns.com/foaf/0.1/name>",
            "Bob"
          ),
          (
            "<http://example.org/bob>",
            "<http://xmlns.com/foaf/0.1/age>",
            "35"
          ),
          (
            "<http://example.org/bob>",
            "<http://xmlns.com/foaf/0.1/mail>",
            "<mailto:bob@work.example.org>"
          )
        ).toDF("s", "p", "o")

        val query =
          """
            |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            |
            |SELECT ?name ?age
            |WHERE {
            |  ?s foaf:name ?name ;
            |     foaf:age ?age .
            |  NOT EXISTS { ?s foaf:mail ?mail }
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 1
        result.right.get.collect.toSet shouldEqual Set(
          Row("\"Alice\"", "23")
        )
      }

      "combined with FILTER" in {

        val df = List(
          (
            "<http://example/alice>",
            "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
            "<http://xmlns.com/foaf/0.1/Person>"
          ),
          (
            "<http://example/alice>",
            "<http://xmlns.com/foaf/0.1/name>",
            "Alice"
          ),
          (
            "<http://example/bob>",
            "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
            "<http://xmlns.com/foaf/0.1/Person>"
          )
        ).toDF("s", "p", "o")

        val query =
          """
            |PREFIX  rdf:    <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            |PREFIX  foaf:   <http://xmlns.com/foaf/0.1/>
            |
            |SELECT ?person
            |WHERE
            |{
            |    ?person rdf:type  foaf:Person .
            |    FILTER NOT EXISTS { ?person foaf:name ?name }
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 1
        result.right.get.collect.toSet shouldEqual Set(
          Row("<http://example/bob>")
        )
      }
    }
  }
}
