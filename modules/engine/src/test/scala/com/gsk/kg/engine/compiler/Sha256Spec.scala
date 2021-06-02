package com.gsk.kg.engine.compiler

import org.apache.spark.sql.Row

import com.gsk.kg.engine.Compiler
import com.gsk.kg.sparqlparser.TestConfig

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class Sha256Spec
    extends AnyWordSpec
    with Matchers
    with SparkSpec
    with TestConfig {

  import sqlContext.implicits._

  /*
   https://www.w3.org/TR/sparql11-query/#func-sha256
   SHA256("abc") -> "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
   SHA256("abc"^^xsd:string) -> "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
   */

  "perform SHA256 function correctly" when {

    "str is simple literal" in {
      val str = "abc"
      val expected = Row(
        "\"ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad\""
      )
      val actual = act(str)
      actual shouldEqual expected
    }

    "str is xsd:string" in {
      val str = "\"abc\"^^xsd:string"
      val expected = Row(
        "\"ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad\""
      )
      val actual = act(str)
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
            ?x foaf:titleHashed ?titleHashed .
          }
          WHERE{
            ?x foaf:title ?title .
            BIND(sha256(?title) as ?titleHashed) .
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
