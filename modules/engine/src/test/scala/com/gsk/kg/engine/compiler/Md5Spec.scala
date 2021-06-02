package com.gsk.kg.engine.compiler

import org.apache.spark.sql.Row

import com.gsk.kg.engine.Compiler
import com.gsk.kg.sparqlparser.TestConfig

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class Md5Spec extends AnyWordSpec with Matchers with SparkSpec with TestConfig {

  import sqlContext.implicits._

  /*
   https://www.w3.org/TR/sparql11-query/#func-md5
   MD5("abc") -> "900150983cd24fb0d6963f7d28e17f72"
   MD5("abc"^^xsd:string) -> "900150983cd24fb0d6963f7d28e17f72"
   */

  "perform MD5 function correctly" when {

    "str is simple literal" in {
      val str      = "abc"
      val expected = Row("\"900150983cd24fb0d6963f7d28e17f72\"")
      val actual   = act(str)
      actual shouldEqual expected
    }

    "str is xsd:string" in {
      val str      = "\"abc\"^^xsd:string"
      val expected = Row("\"900150983cd24fb0d6963f7d28e17f72\"")
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
            BIND(md5(?title) as ?titleHashed) .
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
