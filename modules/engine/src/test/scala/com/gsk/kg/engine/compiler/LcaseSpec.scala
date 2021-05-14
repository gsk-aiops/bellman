package com.gsk.kg.engine.compiler

import org.apache.spark.sql.Row

import com.gsk.kg.engine.Compiler
import com.gsk.kg.sparqlparser.TestConfig

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class LcaseSpec
    extends AnyWordSpec
    with Matchers
    with DataFrameSuiteBase
    with TestConfig {

  import sqlContext.implicits._

  /*
   https://www.w3.org/TR/sparql11-query/#func-lcase
   lcase("BAR") -> "bar"
   lcase("BAR"@en) -> "bar"@en
   lcase("BAR"^^xsd:string) -> "bar"^^xsd:string
   */

  "perform LCASE function correctly" when {

    "str is simple literal" in {
      // lcase("BAR") -> "bar"
      val str      = "BAR"
      val expected = Row("\"bar\"")
      val actual   = act(str)
      actual shouldEqual expected
    }

    "str is plain literal with language tag" in {
      // lcase("BAR"@en) -> "bar"@en
      val str      = "\"BAR\"@en"
      val expected = Row("\"bar\"@en")
      val actual   = act(str)
      actual shouldEqual expected
    }

    "str is xsd:string" in {
      // lcase("BAR"^^xsd:string) -> "bar"^^xsd:string
      val str      = "\"BAR\"^^xsd:string"
      val expected = Row("\"bar\"^^xsd:string")
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
            BIND(LCASE(?title) as ?titleUpper) .
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
