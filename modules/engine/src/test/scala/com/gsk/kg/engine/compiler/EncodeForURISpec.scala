package com.gsk.kg.engine.compiler

import org.apache.spark.sql.Row

import com.gsk.kg.engine.Compiler
import com.gsk.kg.sparqlparser.TestConfig

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class EncodeForURISpec
    extends AnyWordSpec
    with Matchers
    with SparkSpec
    with TestConfig {

  import sqlContext.implicits._

  /*
  https://www.w3.org/TR/sparql11-query/#func-encode
  encode_for_uri("Los Angeles") -> "Los%20Angeles"
  encode_for_uri("Los Angeles"@en) -> "Los%20Angeles"
  encode_for_uri("Los Angeles"^^xsd:string) -> "Los%20Angeles"
   */

  "perform ENCODE_FOR_URI function correctly" when {

    "form is simple literal" in {
      // encode_for_uri("Los Angeles") -> "Los%20Angeles"
      val str      = "Los Angeles"
      val expected = Row("\"Los%20Angeles\"")
      val actual   = act(str)
      actual shouldEqual expected
    }

    "form has language tag" in {
      // encode_for_uri("Los Angeles"@en) -> "Los%20Angeles"
      val str      = "\"Los Angeles\"@en"
      val expected = Row("\"Los%20Angeles\"")
      val actual   = act(str)
      actual shouldEqual expected
    }

    "form is typed string" in {
      // encode_for_uri("Los Angeles"^^xsd:string) -> "Los%20Angeles"
      val str      = "\"Los Angeles\"^^xsd:string"
      val expected = Row("\"Los%20Angeles\"")
      val actual   = act(str)
      actual shouldEqual expected
    }
  }

  private def act(uri: String): Row = {
    val df = List(
      (
        "<http://uri.com/subject/#a1>",
        "<http://xmlns.com/foaf/0.1/title>",
        uri
      )
    ).toDF("s", "p", "o")

    val query =
      s"""
          PREFIX foaf: <http://xmlns.com/foaf/0.1/>
          CONSTRUCT {
            ?x foaf:titleURI ?titleURI .
          }
          WHERE{
            ?x foaf:title ?title .
            BIND(ENCODE_FOR_URI(?title) as ?titleURI) .
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
