package com.gsk.kg.engine.compiler

import org.apache.spark.sql.Row

import com.gsk.kg.engine.Compiler
import com.gsk.kg.sparqlparser.TestConfig

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class UcaseSpec
    extends AnyWordSpec
    with Matchers
    with DataFrameSuiteBase
    with TestConfig {

  import sqlContext.implicits._

  /*
   https://www.w3.org/TR/sparql11-query/#func-ucase
   ucase("foo") -> "FOO"
   ucase("foo"@en) -> "FOO"@en
   ucase("foo"^^xsd:string) -> "FOO"^^xsd:string
   */

  "perform UCASE function correctly" when {

    "str is simple literal" in {
      // ucase("foo") -> "FOO"
      val str      = "foo"
      val expected = Row("\"FOO\"")
      val actual   = act(str)
      actual shouldEqual expected
    }

    "str is plain literal with language tag" in {
      // ucase("foo"@en) -> "FOO"@en
      val str      = "\"foo\"@en"
      val expected = Row("\"FOO\"@en")
      val actual   = act(str)
      actual shouldEqual expected
    }

    "str is xsd:string" in {
      // ucase("foo"^^xsd:string) -> "FOO"^^xsd:string
      val str      = "\"foo\"^^xsd:string"
      val expected = Row("\"FOO\"^^xsd:string")
      val actual   = act(str)
      actual shouldEqual expected
    }
  }

  private def act(str: String): Row = {
    val df = List(
      (
        "<http://uri.com/subject/#a1>",
        "<http://xmlns.com/foaf/0.1/title>",
        str
      )
    ).toDF("s", "p", "o")

    val query =
      s"""
          PREFIX foaf: <http://xmlns.com/foaf/0.1/>
          CONSTRUCT {
            ?x foaf:titleUpper ?titleUpper .
          }
          WHERE{
            ?x foaf:title ?title .
            BIND(UCASE(?title) as ?titleUpper) .
          }
          """

    Compiler
      .compile(df, query, config)
      .right
      .get
      .drop("s", "p")
      .head()
  }

}
