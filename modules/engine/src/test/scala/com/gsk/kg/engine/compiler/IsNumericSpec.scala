package com.gsk.kg.engine.compiler

import org.apache.spark.sql.Row

import com.gsk.kg.engine.Compiler
import com.gsk.kg.sparqlparser.TestConfig

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class IsNumericSpec
    extends AnyWordSpec
    with Matchers
    with DataFrameSuiteBase
    with TestConfig {

  import sqlContext.implicits._

  /*
  https://www.w3.org/TR/sparql11-query/#func-isNumeric
  isNumeric(12) -> true
  isNumeric("12"^^xsd:nonNegativeInteger) -> true
  isNumeric("12") -> false
  isNumeric("1200"^^xsd:byte) -> false
  isNumeric(<http://example/>) -> false
   */

  "perform isNumeric function correctly" when {

    "term is a simple int numeric" in {
      // isNumeric(12) -> true
      val term     = "12"
      val expected = Row("12")
      val actual   = actTrue(term)
      actual shouldEqual expected
    }

    "term is typed nonNegativeInteger" in {
      // isNumeric("12"^^xsd:nonNegativeInteger) -> true
      val term     = "\"12\"^^xsd:nonNegativeInteger"
      val expected = Row("\"12\"^^xsd:nonNegativeInteger")
      val actual   = actTrue(term)
      actual shouldEqual expected
    }

    "term is string literal" in {
      // isNumeric("12") -> false
      val term     = "\"12\""
      val expected = false
      val actual   = actFalse(term)
      actual shouldEqual expected
    }

    "term is typed byte" in {
      // isNumeric("1200"^^xsd:byte) -> false
      val term     = "\"1200\"^^xsd:byte"
      val expected = false
      val actual   = actFalse(term)
      actual shouldEqual expected
    }

    "term is uri" in {
      // isNumeric(<http://example/>) -> false
      val term     = "<http://example/>"
      val expected = false
      val actual   = actFalse(term)
      actual shouldEqual expected
    }
  }

  private def actTrue(term: String): Row = {
    val df = List(
      (
        "<http://uri.com/subject/#a1>",
        "<http://xmlns.com/foaf/0.1/age>",
        term
      )
    ).toDF("s", "p", "o")

    val query =
      s"""
          PREFIX foaf: <http://xmlns.com/foaf/0.1/>
          SELECT ?age
           WHERE { ?x foaf:age ?age .
               FILTER isNumeric(?age) }
          """

    Compiler
      .compile(df, query, config)
      .right
      .get
      .drop("s", "p")
      .head()
  }

  private def actFalse(term: String): Boolean = {
    val df = List(
      (
        "<http://uri.com/subject/#a1>",
        "<http://xmlns.com/foaf/0.1/age>",
        term
      )
    ).toDF("s", "p", "o")

    val query =
      s"""
          PREFIX foaf: <http://xmlns.com/foaf/0.1/>
          SELECT ?age
           WHERE { ?x foaf:age ?age .
               FILTER isNumeric(?age) }
          """

    !Compiler
      .compile(df, query, config)
      .right
      .get
      .drop("s", "p")
      .isEmpty
  }

}
