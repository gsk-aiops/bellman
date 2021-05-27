package com.gsk.kg.engine.compiler

import com.gsk.kg.engine.Compiler
import com.gsk.kg.sparqlparser.TestConfig
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ExistsSpec
    extends AnyWordSpec
    with Matchers
    with DataFrameSuiteBase
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
          ("<http://example.org/alice>", "<http://xmlns.com/foaf/0.1/age>", 23),
          (
            "<http://example.org/bob>",
            "<http://xmlns.com/foaf/0.1/name>",
            "Bob"
          ),
          ("<http://example.org/bob>", "<http://xmlns.com/foaf/0.1/age>", 35),
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
          Row("\"Alice\"", "23")
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
          ("<http://example.org/alice>", "<http://xmlns.com/foaf/0.1/age>", 23),
          (
            "<http://example.org/bob>",
            "<http://xmlns.com/foaf/0.1/name>",
            "Bob"
          ),
          ("<http://example.org/bob>", "<http://xmlns.com/foaf/0.1/age>", 35),
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
          Row("\"Bob\"", "35")
        )
      }
    }
  }
}
