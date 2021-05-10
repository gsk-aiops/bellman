package com.gsk.kg.engine.compiler

import com.gsk.kg.engine.Compiler
import com.gsk.kg.sparqlparser.EngineError
import com.gsk.kg.sparqlparser.TestConfig
import com.holdenkarau.spark.testing.DataFrameSuiteBase
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

    "arg1 is simple literal and arg2 is simple literal" in {
      // strbefore("abc", "b") -> "a"
      val arg1     = "\"abc\""
      val arg2     = "\"b\""
      val expected = "\"a\""

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
          CONSTRUCT {
            ?x foaf:firstName ?firstName .
          }
          WHERE{
            ?x foaf:name ?name .
            BIND(STRBEFORE(?name, $arg2) as ?firstName) .
          }
          """

      val result = Compiler
        .compile(df, query, config)
        .right
        .get
        .drop("s", "p")
        .first()
        .getString(0)
      result shouldEqual expected
    }

    "arg1 is plain literal with language tag and arg2 is simple literal" in {
      // strbefore("abc"@en, "bc") -> "a"@en
      val arg1     = "\"abc\"@en"
      val arg2     = "\"bc\""
      val expected = "\"a\"@en"

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
          CONSTRUCT {
            ?x foaf:firstName ?firstName .
          }
          WHERE{
            ?x foaf:name ?name .
            BIND(STRBEFORE(?name, $arg2) as ?firstName) .
          }
          """

      val result = Compiler
        .compile(df, query, config)
        .right
        .get
        .drop("s", "p")
        .first()
        .getString(0)
      result shouldEqual expected
    }

    "arg1 is plain literal with language tag and arg2 is plain literal with incompatible language tag" in {
      // strbefore("abc"@en, "b"@cy) -> error
      val arg1     = "\"abc\"@en"
      val arg2     = "\"b\"@cy"
      val expected = EngineError

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
          CONSTRUCT {
            ?x foaf:firstName ?firstName .
          }
          WHERE{
            ?x foaf:name ?name .
            BIND(STRBEFORE(?name, $arg2) as ?firstName) .
          }
          """

      val result = Compiler.compile(df, query, config).left.get
      result shouldEqual expected
    }

    "arg1 is xsd:string and arg two is empty string simple literal" in {
      // strbefore("abc"^^xsd:string, "") -> ""^^xsd:string
      val arg1     = "\"abc\"^^xsd:string"
      val arg2     = "\"\""
      val expected = "\"\"^^xsd:string"

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
          CONSTRUCT {
            ?x foaf:firstName ?firstName .
          }
          WHERE{
            ?x foaf:name ?name .
            BIND(STRBEFORE(?name, $arg2) as ?firstName) .
          }
          """

      val result = Compiler
        .compile(df, query, config)
        .right
        .get
        .drop("s", "p")
        .first()
        .getString(0)
      result shouldEqual expected
    }

    "arg1 is simple literal and arg two is simple literal" in {
      // strbefore("abc","xyz") -> ""
      val arg1     = "\"abc\""
      val arg2     = "\"xyz\""
      val expected = "\"\""

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
          CONSTRUCT {
            ?x foaf:firstName ?firstName .
          }
          WHERE{
            ?x foaf:name ?name .
            BIND(STRBEFORE(?name, $arg2) as ?firstName) .
          }
          """

      val result = Compiler
        .compile(df, query, config)
        .right
        .get
        .drop("s", "p")
        .first()
        .getString(0)
      result shouldEqual expected
    }
  }

  "arg1 is plain literal with language tag and arg2 is plain literal with compatible language tag" in {
    // strbefore("abc"@en, "z"@en) -> ""
    val arg1     = "\"abc\"@en"
    val arg2     = "\"z\"@en"
    val expected = "\"\""

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
          CONSTRUCT {
            ?x foaf:firstName ?firstName .
          }
          WHERE{
            ?x foaf:name ?name .
            BIND(STRBEFORE(?name, $arg2) as ?firstName) .
          }
          """

    val result = Compiler
      .compile(df, query, config)
      .right
      .get
      .drop("s", "p")
      .first()
      .getString(0)
    result shouldEqual expected
  }

  "arg1 is plain literal with language tag and arg2 is simple literal" in {
    // strbefore("abc"@en, "z") -> ""
    val arg1     = "\"abc\"@en"
    val arg2     = "\"z\""
    val expected = "\"\""

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
          CONSTRUCT {
            ?x foaf:firstName ?firstName .
          }
          WHERE{
            ?x foaf:name ?name .
            BIND(STRBEFORE(?name, $arg2) as ?firstName) .
          }
          """

    val result = Compiler
      .compile(df, query, config)
      .right
      .get
      .drop("s", "p")
      .first()
      .getString(0)
    result shouldEqual expected
  }

  "arg1 is plain literal with language tag and arg2 is empty string plain literal with compatible language tag" in {
    // strbefore("abc"@en, ""@en) -> ""@en
    val arg1     = "\"abc\"@en"
    val arg2     = "\"\"@en"
    val expected = "\"\"@en"

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
          CONSTRUCT {
            ?x foaf:firstName ?firstName .
          }
          WHERE{
            ?x foaf:name ?name .
            BIND(STRBEFORE(?name, $arg2) as ?firstName) .
          }
          """

    val result = Compiler
      .compile(df, query, config)
      .right
      .get
      .drop("s", "p")
      .first()
      .getString(0)
    result shouldEqual expected
  }

  "arg1 is plain literal with language tag and arg2 is empty simple string" in {
    // strbefore("abc"@en, "") -> ""@en
    val arg1     = "\"abc\"@en"
    val arg2     = "\"\""
    val expected = "\"\"@en"

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
          CONSTRUCT {
            ?x foaf:firstName ?firstName .
          }
          WHERE{
            ?x foaf:name ?name .
            BIND(STRBEFORE(?name, $arg2) as ?firstName) .
          }
          """

    val result = Compiler
      .compile(df, query, config)
      .right
      .get
      .drop("s", "p")
      .first()
      .getString(0)
    result shouldEqual expected
  }
}
