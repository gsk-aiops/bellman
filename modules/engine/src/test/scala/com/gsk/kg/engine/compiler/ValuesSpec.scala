package com.gsk.kg.engine.compiler

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row

import com.gsk.kg.engine.Compiler
import com.gsk.kg.sparqlparser.TestConfig

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ValuesSpec
    extends AnyWordSpec
    with Matchers
    with DataFrameSuiteBase
    with TestConfig {

  import sqlContext.implicits._

  "perform VALUES queries" should {

    "execute and obtain expected results" when {

      "only values with no other clauses" in {

        val df: DataFrame = List(
          (
            "<http://example.org/book/book1>",
            "<http://purl.org/dc/elements/1.1/title>",
            "SPARQL Tutorial"
          ),
          (
            "<http://example.org/book/book1>",
            "<http://example.org/ns#price>",
            "42"
          ),
          (
            "<http://example.org/book/book2>",
            "<http://purl.org/dc/elements/1.1/title>",
            "The Semantic Web"
          ),
          (
            "<http://example.org/book/book2>",
            "<http://example.org/ns#price>",
            "23"
          )
        ).toDF("s", "p", "o")

        val query =
          """
            |PREFIX dc:   <http://purl.org/dc/elements/1.1/> 
            |PREFIX :     <http://example.org/book/> 
            |PREFIX ns:   <http://example.org/ns#>
            |
            |SELECT ?book ?title
            |{
            |   VALUES (?book ?title)
            |   { (UNDEF "SPARQL Tutorial")
            |     (:book2 UNDEF)
            |   }
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 2
        result.right.get.collect.toSet shouldEqual Set(
          Row(null, "\"SPARQL Tutorial\""),
          Row("<http://example.org/book/book2>", null)
        )
      }

      "single variable on clause" in {

        val df: DataFrame = List(
          (
            "<http://example.org/book/book1>",
            "<http://purl.org/dc/elements/1.1/title>",
            "SPARQL Tutorial"
          ),
          (
            "<http://example.org/book/book1>",
            "<http://example.org/ns#price>",
            "42"
          ),
          (
            "<http://example.org/book/book2>",
            "<http://purl.org/dc/elements/1.1/title>",
            "The Semantic Web"
          ),
          (
            "<http://example.org/book/book2>",
            "<http://example.org/ns#price>",
            "23"
          )
        ).toDF("s", "p", "o")

        val query =
          """
            |PREFIX dc:   <http://purl.org/dc/elements/1.1/> 
            |PREFIX :     <http://example.org/book/> 
            |PREFIX ns:   <http://example.org/ns#> 
            |
            |SELECT ?book ?title ?price
            |{
            |   VALUES ?book { :book1 :book3 }
            |   ?book dc:title ?title ;
            |         ns:price ?price .
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 1
        result.right.get.collect.toSet shouldEqual Set(
          Row("<http://example.org/book/book1>", "\"SPARQL Tutorial\"", "42")
        )
      }

      "multiple variables on clause" in {

        val df: DataFrame = List(
          (
            "<http://example.org/book/book1>",
            "<http://purl.org/dc/elements/1.1/title>",
            "SPARQL Tutorial"
          ),
          (
            "<http://example.org/book/book1>",
            "<http://example.org/ns#price>",
            "42"
          ),
          (
            "<http://example.org/book/book2>",
            "<http://purl.org/dc/elements/1.1/title>",
            "The Semantic Web"
          ),
          (
            "<http://example.org/book/book2>",
            "<http://example.org/ns#price>",
            "23"
          )
        ).toDF("s", "p", "o")

        val query =
          """
            |PREFIX dc:   <http://purl.org/dc/elements/1.1/> 
            |PREFIX :     <http://example.org/book/> 
            |PREFIX ns:   <http://example.org/ns#> 
            |
            |SELECT ?book ?title ?price
            |{
            |   ?book dc:title ?title ;
            |         ns:price ?price .
            |   VALUES (?book ?title)
            |   { (:book1 "SPARQL Tutorial")
            |     (:book2 "The Semantic Web")
            |   }
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 2
        result.right.get.collect.toSet shouldEqual Set(
          Row("<http://example.org/book/book1>", "\"SPARQL Tutorial\"", "42"),
          Row("<http://example.org/book/book2>", "\"The Semantic Web\"", "23")
        )
      }

      // TODO: Un-ignore when UNDEF supported
      // See: https://github.com/gsk-aiops/bellman/issues/390
      "multiple variables on clause and UNDEF values" ignore {

        val df: DataFrame = List(
          (
            "<http://example.org/book/book1>",
            "<http://purl.org/dc/elements/1.1/title>",
            "SPARQL Tutorial"
          ),
          (
            "<http://example.org/book/book1>",
            "<http://example.org/ns#price>",
            "42"
          ),
          (
            "<http://example.org/book/book2>",
            "<http://purl.org/dc/elements/1.1/title>",
            "The Semantic Web"
          ),
          (
            "<http://example.org/book/book2>",
            "<http://example.org/ns#price>",
            "23"
          )
        ).toDF("s", "p", "o")

        val query =
          """
            |PREFIX dc:   <http://purl.org/dc/elements/1.1/> 
            |PREFIX :     <http://example.org/book/> 
            |PREFIX ns:   <http://example.org/ns#> 
            |
            |SELECT ?book ?title ?price
            |{
            |   ?book dc:title ?title ;
            |         ns:price ?price .
            |   VALUES (?book ?title)
            |   { (UNDEF "SPARQL Tutorial")
            |     (:book2 UNDEF)
            |   }
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 2
        result.right.get.collect.toSet shouldEqual Set(
          Row("<http://example.org/book/book1>", "\"SPARQL Tutorial\"", "42"),
          Row("<http://example.org/book/book2>", "\"The Semantic Web\"", "23")
        )
      }

      "performed over SELECT statement results" in {

        val df: DataFrame = List(
          (
            "<http://example.org/book/book1>",
            "<http://purl.org/dc/elements/1.1/title>",
            "SPARQL Tutorial"
          ),
          (
            "<http://example.org/book/book1>",
            "<http://example.org/ns#price>",
            "42"
          ),
          (
            "<http://example.org/book/book2>",
            "<http://purl.org/dc/elements/1.1/title>",
            "The Semantic Web"
          ),
          (
            "<http://example.org/book/book2>",
            "<http://example.org/ns#price>",
            "23"
          )
        ).toDF("s", "p", "o")

        val query =
          """
            |PREFIX dc:   <http://purl.org/dc/elements/1.1/> 
            |PREFIX :     <http://example.org/book/> 
            |PREFIX ns:   <http://example.org/ns#> 
            |
            |SELECT ?book ?title ?price
            |{
            |   ?book dc:title ?title ;
            |         ns:price ?price .
            |}
            |VALUES (?book ?title)
            |{ (:book1 "SPARQL Tutorial")
            |  (:book2 "The Semantic Web")
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 2
        result.right.get.collect.toSet shouldEqual Set(
          Row("<http://example.org/book/book1>", "\"SPARQL Tutorial\"", "42"),
          Row("<http://example.org/book/book2>", "\"The Semantic Web\"", "23")
        )
      }
    }
  }
}
