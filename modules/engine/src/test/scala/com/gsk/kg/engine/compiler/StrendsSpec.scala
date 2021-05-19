package com.gsk.kg.engine.compiler

import org.apache.spark.sql.Row

import com.gsk.kg.engine.Compiler
import com.gsk.kg.sparqlparser.TestConfig

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class StrendsSpec
    extends AnyWordSpec
    with Matchers
    with DataFrameSuiteBase
    with TestConfig {

  import sqlContext.implicits._

  /*
  https://www.w3.org/TR/sparql11-query/#func-strends
  strEnds("foobar", "bar") -> true
  strEnds("foobar"@en, "bar"@en) -> true
  strEnds("foobar"^^xsd:string, "bar"^^xsd:string) -> true
  strEnds("foobar"^^xsd:string, "bar") -> true
  strEnds("foobar", "bar"^^xsd:string) -> true
  strEnds("foobar"@en, "bar") -> true
  strEnds("foobar"@en, "bar"^^xsd:string) -> true
   */

  "perform STRENDS function correctly" when {

    "arg1 is simple string and arg2 is simple string" in {
      // strEnds("foobar", "bar") -> true
      val arg1     = "foobar"
      val arg2     = "\"bar\""
      val expected = Row("\"foobar\"")
      val actual   = act(arg1, arg2)
      actual shouldEqual expected
    }

    "arg1 is plain literal with language tag and arg2 is plain literal with language tag" in {
      // strEnds("foobar"@en, "bar"@en) -> true
      val arg1     = "\"foobar\"@en"
      val arg2     = "\"bar\"@en"
      val expected = Row("\"foobar\"@en")
      val actual   = act(arg1, arg2)
      actual shouldEqual expected
    }

    "arg1 is typed string and arg2 is typed string" in {
      // strEnds("foobar"^^xsd:string, "bar"^^xsd:string) -> true
      val arg1     = "\"foobar\"^^xsd:string"
      val arg2     = "\"bar\"^^xsd:string"
      val expected = Row("\"foobar\"^^xsd:string")
      val actual   = act(arg1, arg2)
      actual shouldEqual expected
    }

    "arg1 is typed string and arg2 is simple string" in {
      // strEnds("foobar"^^xsd:string, "bar") -> true
      val arg1     = "\"foobar\"^^xsd:string"
      val arg2     = "\"bar\""
      val expected = Row("\"foobar\"^^xsd:string")
      val actual   = act(arg1, arg2)
      actual shouldEqual expected
    }

    "arg1 is simple string and arg2 is typed string" in {
      // strEnds("foobar", "bar"^^xsd:string) -> true
      val arg1     = "foobar"
      val arg2     = "\"bar\"^^xsd:string"
      val expected = Row("\"foobar\"")
      val actual   = act(arg1, arg2)
      actual shouldEqual expected
    }

    "arg1 is plain literal with language tag and arg2 is simple string" in {
      // strEnds("foobar"@en, "bar") -> true
      val arg1     = "\"foobar\"@en"
      val arg2     = "\"bar\""
      val expected = Row("\"foobar\"@en")
      val actual   = act(arg1, arg2)
      actual shouldEqual expected
    }

    "arg1 is plain literal with language tag and arg2 is typed string" in {
      // strEnds("foobar"@en, "bar"^^xsd:string) -> true
      val arg1     = "\"foobar\"@en"
      val arg2     = "\"bar\"^^xsd:string"
      val expected = Row("\"foobar\"@en")
      val actual   = act(arg1, arg2)
      actual shouldEqual expected
    }
  }

  private def act(arg1: String, arg2: String): Row = {
    val df = List(
      (
        "<http://uri.com/subject/#a1>",
        "<http://xmlns.com/foaf/0.1/name>",
        arg1
      )
    ).toDF("s", "p", "o")

    val query =
      s"""
          PREFIX foaf: <http://xmlns.com/foaf/0.1/>
          PREFIX  xsd:  <http://www.w3.org/2001/XMLSchema#>
          SELECT ?name
           WHERE { ?x foaf:name ?name
               FILTER STRENDS(?name, $arg2) }
          """

    Compiler
      .compile(df, query, config)
      .right
      .get
      .drop("s", "p")
      .head()
  }
}
