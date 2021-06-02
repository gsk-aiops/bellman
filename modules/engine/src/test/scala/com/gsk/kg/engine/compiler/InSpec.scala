package com.gsk.kg.engine.compiler

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row

import com.gsk.kg.engine.Compiler
import com.gsk.kg.sparqlparser.TestConfig

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class InSpec
    extends AnyWordSpec
    with Matchers
    with DataFrameSuiteBase
    with TestConfig {

  import sqlContext.implicits._

  "perform IN query" should {

    "execute and obtain expected results" when {

      "simple query with IN" in {

        val df: DataFrame = List(
          (
            "<http://example.org/Star_wars>",
            "<http://xmlns.com/foaf/0.1/title>",
            "\"Star Wars\"@en"
          ),
          (
            "<http://example.org/Star_wars>",
            "<http://xmlns.com/foaf/0.1/title>",
            "\"La guerra de las galaxias\"@es"
          ),
          (
            "<http://example.org/Star_wars>",
            "<http://xmlns.com/foaf/0.1/title>",
            "\"sterrenoorlogen\"@de"
          )
        ).toDF("s", "p", "o")

        val query =
          """
            |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            |
            |SELECT ?title
            |WHERE {
            | ?film foaf:title ?title .
            | FILTER (lang(?title) IN ("en", "es"))
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 2
        result.right.get.collect.toSet shouldEqual Set(
          Row("\"Star Wars\"@en"),
          Row("\"La guerra de las galaxias\"@es")
        )
      }
    }
  }

  "perform NOT IN query" should {

    "execute and obtain expected results" when {

      "simple query with NOT IN" in {

        val df: DataFrame = List(
          (
            "<http://example.org/Star_wars>",
            "<http://xmlns.com/foaf/0.1/title>",
            "\"Star Wars\"@en"
          ),
          (
            "<http://example.org/Star_wars>",
            "<http://xmlns.com/foaf/0.1/title>",
            "\"La guerra de las galaxias\"@es"
          ),
          (
            "<http://example.org/Star_wars>",
            "<http://xmlns.com/foaf/0.1/title>",
            "\"sterrenoorlogen\"@de"
          )
        ).toDF("s", "p", "o")

        val query =
          """
            |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            |
            |SELECT ?title
            |WHERE {
            | ?film foaf:title ?title .
            | FILTER (lang(?title) NOT IN ("en", "es"))
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 1
        result.right.get.collect.toSet shouldEqual Set(
          Row("\"sterrenoorlogen\"@de")
        )
      }
    }
  }

}
