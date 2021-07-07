package com.gsk.kg.engine.compiler

import org.apache.spark.sql.Row

import com.gsk.kg.engine.Compiler
import com.gsk.kg.sparqlparser.TestConfig

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class StrlangSpec
  extends AnyWordSpec
    with Matchers
    with SparkSpec
    with TestConfig {

  import sqlContext.implicits._

  "perform STRLANG function correctly" should {

    "tag is es" in {
      val str      = "\"chat\""
      val tag      = "es"
      val expected = Row(s"$str@$tag")
      val actual   = act(str, s"\"$tag\"")
      actual shouldEqual expected
    }

    "tag is en-US" in {
      val str      = "\"chat\""
      val tag      = "en-US"
      val expected = Row(s"$str@$tag")
      val actual   = act(str, s"\"$tag\"")
      actual shouldEqual expected
    }

  }

  private def act(str: String, tag: String): Row = {
    val df = List(
      (
        "<http://uri.com/subject/#a1>",
        "<http://xmlns.com/foaf/0.1/title>",
        tag
      ),
      (
        "<http://uri.com/subject/#a1>",
        "<http://xmlns.com/foaf/0.1/name>",
        str
      )
    ).toDF("s", "p", "o")

    val query =
      s"""
        PREFIX foaf: <http://xmlns.com/foaf/0.1/>
        CONSTRUCT {
          ?x foaf:strlang ?strlang .
        }
        WHERE {
          ?x foaf:title ?title .
          ?x foaf:name ?name .
          BIND(STRLANG(?name, ?title) as ?strlang) .
        }"""

    Compiler
      .compile(df, query, config)
      .right
      .get
      .head()

  }
}
