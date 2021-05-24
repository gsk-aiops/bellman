package com.gsk.kg.engine.compiler

import org.apache.spark.sql.Row

import com.gsk.kg.engine.Compiler
import com.gsk.kg.sparqlparser.TestConfig

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class LangSpec
    extends AnyWordSpec
    with Matchers
    with DataFrameSuiteBase
    with TestConfig {

  import sqlContext.implicits._

  "perform LANG function correctly" when {

    "tag is en" in {
      val str      = "\"BAR\"@en"
      val expected = Row("\"en\"")
      val actual   = act(str)
      actual shouldEqual expected
    }

    "tag is es" in {
      val str      = "\"BAR\"@es"
      val expected = Row("\"es\"")
      val actual   = act(str)
      actual shouldEqual expected
    }

    "tag is en-US" in {
      val str      = "\"BAR\"@en-US"
      val expected = Row("\"en-US\"")
      val actual   = act(str)
      actual shouldEqual expected
    }

    "literal has no tag" in {
      val str      = "BAR"
      val expected = Row("\"\"")
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
            ?x foaf:langTag ?langTag .
          }
          WHERE{
            ?x foaf:title ?title .
            BIND(LANG(?title) as ?langTag) .
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
