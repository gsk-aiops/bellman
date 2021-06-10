package com.gsk.kg.engine.compiler

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row

import com.gsk.kg.engine.Compiler
import com.gsk.kg.sparqlparser.TestConfig

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class BoundSpec
    extends AnyWordSpec
    with Matchers
    with SparkSpec
    with TestConfig {

  import sqlContext.implicits._

  "perform BOUND query" should {

    "execute and obtain expected results" when {

      "bound variable" in {

        val df: DataFrame = List(
          (
            "_:a",
            "<http://xmlns.com/foaf/0.1/givenName>",
            "Alice"
          ),
          (
            "_:b",
            "<http://xmlns.com/foaf/0.1/givenName>",
            "Bob"
          ),
          (
            "_:b",
            "<http://purl.org/dc/elements/1.1/date>",
            "\"2005-04-04T04:04:04Z\"^^<http://www.w3.org/2001/XMLSchema#dateTime>"
          )
        ).toDF("s", "p", "o")

        val query =
          """
            |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            |PREFIX dc:   <http://purl.org/dc/elements/1.1/>
            |PREFIX xsd:   <http://www.w3.org/2001/XMLSchema#>
            |
            |SELECT ?givenName
            |WHERE {
            | ?x foaf:givenName ?givenName .
            | OPTIONAL { ?x dc:date ?date } .
            | FILTER ( bound(?date) )
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 1
        result.right.get.collect.toSet shouldEqual Set(
          Row("\"Bob\"")
        )
      }

      "not bound variable" in {

        val df: DataFrame = List(
          (
            "_:a",
            "<http://xmlns.com/foaf/0.1/givenName>",
            "Alice"
          ),
          (
            "_:b",
            "<http://xmlns.com/foaf/0.1/givenName>",
            "Bob"
          ),
          (
            "_:b",
            "<http://purl.org/dc/elements/1.1/date>",
            "\"2005-04-04T04:04:04Z\"^^<http://www.w3.org/2001/XMLSchema#dateTime>"
          )
        ).toDF("s", "p", "o")

        val query =
          """
            PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            |PREFIX dc:   <http://purl.org/dc/elements/1.1/>
            |PREFIX xsd:   <http://www.w3.org/2001/XMLSchema#>
            |
            |SELECT ?givenName
            |WHERE {
            | ?x foaf:givenName ?givenName .
            | OPTIONAL { ?x dc:date ?date } .
            | FILTER ( !bound(?date) )
            |}
            |""".stripMargin

        val result = Compiler.compile(df, query, config)

        result shouldBe a[Right[_, _]]
        result.right.get.collect.length shouldEqual 1
        result.right.get.collect.toSet shouldEqual Set(
          Row("\"Alice\"")
        )
      }
    }
  }
}
