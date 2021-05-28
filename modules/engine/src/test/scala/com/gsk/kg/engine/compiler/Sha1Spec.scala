package com.gsk.kg.engine.compiler

import org.apache.spark.sql.Row

import com.gsk.kg.engine.Compiler
import com.gsk.kg.sparqlparser.TestConfig

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class Sha1Spec
    extends AnyWordSpec
    with Matchers
    with DataFrameSuiteBase
    with TestConfig {

  import sqlContext.implicits._

  /*
   https://www.w3.org/TR/sparql11-query/#func-sha1
   SHA1("abc") -> "a9993e364706816aba3e25717850c26c9cd0d89d"
   SHA1("abc"^^xsd:string) -> "a9993e364706816aba3e25717850c26c9cd0d89d"
   */

  "perform SHA1 function correctly" when {

    "str is simple literal" in {
      val str      = "abc"
      val expected = Row("\"a9993e364706816aba3e25717850c26c9cd0d89d\"")
      val actual   = act(str)
      actual shouldEqual expected
    }

    "str is xsd:string" in {
      val str      = "\"abc\"^^xsd:string"
      val expected = Row("\"a9993e364706816aba3e25717850c26c9cd0d89d\"")
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
            ?x foaf:titleHashed ?titleHashed .
          }
          WHERE{
            ?x foaf:title ?title .
            BIND(sha1(?title) as ?titleHashed) .
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
