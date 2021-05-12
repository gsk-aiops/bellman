package com.gsk.kg.engine.compiler

import com.gsk.kg.engine.Compiler
import com.gsk.kg.sparqlparser.EngineError
import com.gsk.kg.sparqlparser.TestConfig
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.Row
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class StrbeforeSpec
    extends AnyWordSpec
    with Matchers
    with DataFrameSuiteBase
    with TestConfig {

  import sqlContext.implicits._

  /*
   https://www.w3.org/TR/sparql11-query/#func-strbefore
   strbefore("abc","b") -> "a"
   strbefore("abc"@en,"bc") -> "a"@en
   strbefore("abc"@en,"b"@cy) -> error
   strbefore("abc"^^xsd:string,"") -> ""^^xsd:string
   strbefore("abc","xyz") -> ""
   strbefore("abc"@en, "z"@en) -> ""
   strbefore("abc"@en, "z") -> ""
   strbefore("abc"@en, ""@en) -> ""@en
   strbefore("abc"@en, "") -> ""@en
   */

  "perform STRBEFORE function correctly" when {

    // Passing
    // QUESTION: If input arg is not wrapped in quotes, why would the return value be wrapped in quotes?
    "arg1 is simple literal and arg2 is simple literal" in {
      // strbefore("abc", "b") -> "a"
      val arg1     = "abc"
      val arg2     = "\"b\""
      val expected = Row("\"a\"")
      val actual   = act(arg1, arg2)
      actual shouldEqual expected
    }

    // Failing
    // Expected :["a"@en]
    // Actual   :["""a@en"]
    "arg1 is plain literal with language tag and arg2 is simple literal" in {
      // strbefore("abc"@en, "bc") -> "a"@en
      val arg1     = "\"abc\"@en"
      val arg2     = "\"bc\""
      val expected = Row("\"a\"@en")
      val actual   = act(arg1, arg2)
      actual shouldEqual expected
    }

    // Failing
    // Expected :UnExpectedType
    // Actual   :[null]
    "arg1 is plain literal with language tag and arg2 is plain literal with incompatible language tag" in {
      // strbefore("abc"@en, "b"@cy) -> error
      val arg1     = "\"abc\"@en"
      val arg2     = "\"b\"@cy"
      val expected = EngineError.UnExpectedType
      val actual   = act(arg1, arg2)
      actual shouldEqual expected
    }

    // Failing
    // Expected :[""^^xsd:string]
    // Actual   :["abc"^^xsd:string]
    "arg1 is xsd:string and arg two is empty string simple literal" in {
      // strbefore("abc"^^xsd:string, "") -> ""^^xsd:string
      val arg1     = "\"abc\"^^xsd:string"
      val arg2     = "\"\""
      val expected = Row("\"\"^^xsd:string")
      val actual   = act(arg1, arg2)
      actual shouldEqual expected
    }

    // Passing
    // QUESTION: If input arg is not wrapped in quotes, why would the return value be wrapped in quotes?
    "arg1 is simple literal and arg two is simple literal" in {
      // strbefore("abc","xyz") -> ""
      val arg1     = "abc"
      val arg2     = "\"xyz\""
      val expected = Row("\"\"")
      val actual   = act(arg1, arg2)
      actual shouldEqual expected
    }
  }

  // Passing
  "arg1 is plain literal with language tag and arg2 is plain literal with compatible language tag" in {
    // strbefore("abc"@en, "z"@en) -> ""
    val arg1     = "\"abc\"@en"
    val arg2     = "\"z\"@en"
    val expected = Row("\"\"")
    val actual   = act(arg1, arg2)
    actual shouldEqual expected
  }

  // Passing
  "arg1 is plain literal with language tag and arg2 is simple literal" in {
    // strbefore("abc"@en, "z") -> ""
    val arg1     = "\"abc\"@en"
    val arg2     = "\"z\""
    val expected = Row("\"\"")
    val actual   = act(arg1, arg2)
    actual shouldEqual expected
  }

  // Failing
  // Expected :[""@en]
  // Actual   :["abc"@en]
  "arg1 is plain literal with language tag and arg2 is empty string plain literal with compatible language tag" in {
    // strbefore("abc"@en, ""@en) -> ""@en
    val arg1     = "\"abc\"@en"
    val arg2     = "\"\"@en"
    val expected = Row("\"\"@en")
    val actual   = act(arg1, arg2)
    actual shouldEqual expected
  }

  // Failing
  // Expected :[""@en]
  // Actual   :["abc"@en]
  "arg1 is plain literal with language tag and arg2 is empty simple string" in {
    // strbefore("abc"@en, "") -> ""@en
    val arg1     = "\"abc\"@en"
    val arg2     = "\"\""
    val expected = Row("\"\"@en")
    val actual   = act(arg1, arg2)
    actual shouldEqual expected
  }

  private def act(arg1: String, arg2: String): Row = {
    val df = List(
      (
        "<http://uri.com/subject/#a1>",
        "<http://xmlns.com/foaf/0.1/title>",
        arg1
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
            BIND(STRBEFORE(?title, $arg2) as ?titlePrefix) .
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
