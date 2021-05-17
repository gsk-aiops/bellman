package com.gsk.kg.engine.compiler

import org.apache.spark.sql.Row

import com.gsk.kg.engine.Compiler
import com.gsk.kg.sparqlparser.TestConfig

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class SubstrSpec
    extends AnyWordSpec
    with Matchers
    with DataFrameSuiteBase
    with TestConfig {

  import sqlContext.implicits._

  /*
  https://www.w3.org/TR/sparql11-query/#func-substr
  substr("foobar", 4) -> "bar"
  substr("foobar"@en, 4) -> "bar"@en
  substr("foobar"^^xsd:string, 4) -> "bar"^^xsd:string
  substr("foobar", 4, 1) -> "b"
  substr("foobar"@en, 4, 1) -> "b"@en
  substr("foobar"^^xsd:string, 4, 1) -> "b"^^xsd:string
   */

  "perform SUBSTR function correctly" when {

    "str is simple string and only pos is specified" in {
      // substr("foobar", 4) -> "bar"
      val str      = "foobar"
      val pos      = 4
      val expected = Row("\"bar\"")
      val actual   = actWithPos(str, pos)
      actual shouldEqual expected
    }

    "str is plain literal with language tag and only pos is specified" in {
      // substr("foobar"@en, 4) -> "bar"@en
      val str      = "\"foobar\"@en"
      val pos      = 4
      val expected = Row("\"bar\"@en")
      val actual   = actWithPos(str, pos)
      actual shouldEqual expected
    }

    "str is typed string and only pos is specified" in {
      // substr("foobar"^^xsd:string, 4) -> "bar"^^xsd:string
      val str      = "\"foobar\"^^xsd:string"
      val pos      = 4
      val expected = Row("\"bar\"^^xsd:string")
      val actual   = actWithPos(str, pos)
      actual shouldEqual expected
    }

    "str is simple string and pos and len are specified" in {
      // substr("foobar", 4, 1) -> "b"
      val str      = "foobar"
      val pos      = 4
      val len      = 1
      val expected = Row("\"b\"")
      val actual   = actWithPosLen(str, pos, len)
      actual shouldEqual expected
    }

    "str is plain literal with language tag and pos and len are specified" in {
      // substr("foobar"@en, 4, 1) -> "b"@en
      val str      = "\"foobar\"@en"
      val pos      = 4
      val len      = 1
      val expected = Row("\"b\"@en")
      val actual   = actWithPosLen(str, pos, len)
      actual shouldEqual expected
    }

    "str is typed string and pos and len are specified" in {
      // substr("foobar"^^xsd:string, 4, 1) -> "b"^^xsd:string
      val str      = "\"foobar\"^^xsd:string"
      val pos      = 4
      val len      = 1
      val expected = Row("\"b\"^^xsd:string")
      val actual   = actWithPosLen(str, pos, len)
      actual shouldEqual expected
    }
  }

  private def actWithPos(str: String, pos: Int): Row = {
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
            ?x foaf:titlePrefix ?titlePrefix .
          }
          WHERE{
            ?x foaf:title ?title .
            BIND(SUBSTR(?title, $pos) as ?titlePrefix) .
          }
          """

    Compiler
      .compile(df, query, config)
      .right
      .get
      .drop("s", "p")
      .head()
  }

  private def actWithPosLen(str: String, pos: Int, len: Int): Row = {
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
            ?x foaf:titlePrefix ?titlePrefix .
          }
          WHERE{
            ?x foaf:title ?title .
            BIND(SUBSTR(?title, $pos, $len) as ?titlePrefix) .
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
