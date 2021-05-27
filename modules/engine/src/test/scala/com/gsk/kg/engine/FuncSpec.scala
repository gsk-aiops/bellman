package com.gsk.kg.engine

import cats.syntax.list._

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._

import com.gsk.kg.engine.scalacheck.CommonGenerators

import java.time.LocalDateTime
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.time.temporal.TemporalAccessor

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class FuncSpec
    extends AnyWordSpec
    with Matchers
    with DataFrameSuiteBase
    with ScalaCheckDrivenPropertyChecks
    with CommonGenerators {

  import sqlContext.implicits._

  override implicit def reuseContextIfPossible: Boolean = true

  override implicit def enableHiveSupport: Boolean = false

  "Func.negate" should {

    "return the input boolean column negated" in {

      val df = List(
        true,
        false
      ).toDF("boolean")

      val result = df.select(Func.negate(df("boolean"))).collect

      result shouldEqual Array(
        Row(false),
        Row(true)
      )
    }

    "fail when the input column contain values that are not boolean values" in {

      val df = List(
        "a",
        null
      ).toDF("boolean")

      val caught = intercept[AnalysisException] {
        df.select(Func.negate(df("boolean"))).collect
      }

      caught.getMessage should contain
      "cannot resolve '(NOT `boolean`)' due to data type mismatch"
    }
  }

  "Func.isBlank" should {

    "return whether a node is a blank node or not" in {

      val df = List(
        "_:a",
        "a:a",
        "_:1",
        "1:1",
        "foaf:name",
        "_:name"
      ).toDF("text")

      val result = df.select(Func.isBlank(df("text"))).collect

      result shouldEqual Array(
        Row(true),
        Row(false),
        Row(true),
        Row(false),
        Row(false),
        Row(true)
      )
    }
  }

  "Func.replace" should {

    "replace when pattern occurs" in {

      val df = List(
        "abcd",
        "abaB",
        "bbBB",
        "aaaa"
      ).toDF("text")

      val result = df.select(Func.replace(df("text"), "b", "Z", "")).collect

      result shouldEqual Array(
        Row("aZcd"),
        Row("aZaB"),
        Row("ZZBB"),
        Row("aaaa")
      )
    }

    "replace(abracadabra, bra, *) returns a*cada*" in {

      val df = List("abracadabra").toDF("text")

      val result = df.select(Func.replace(df("text"), "bra", "*", "")).collect

      result shouldEqual Array(
        Row("a*cada*")
      )
    }

    "replace(abracadabra, a.*a, *) returns *" in {

      val df = List("abracadabra").toDF("text")

      val result = df.select(Func.replace(df("text"), "a.*a", "*", "")).collect

      result shouldEqual Array(
        Row("*")
      )
    }

    "replace(abracadabra, a.*?a, *) returns *c*bra" in {

      val df = List("abracadabra").toDF("text")

      val result = df.select(Func.replace(df("text"), "a.*?a", "*", "")).collect

      result shouldEqual Array(
        Row("*c*bra")
      )
    }

    "replace(abracadabra, a, \"\") returns brcdbr" in {

      val df = List("abracadabra").toDF("text")

      val result = df.select(Func.replace(df("text"), "a", "", "")).collect

      result shouldEqual Array(
        Row("brcdbr")
      )
    }

    "replace(abracadabra, a(.), a$1$1) returns abbraccaddabbra" in {

      val df = List("abracadabra").toDF("text")

      val result =
        df.select(Func.replace(df("text"), "a(.)", "a$1$1", "")).collect

      result shouldEqual Array(
        Row("abbraccaddabbra")
      )
    }

    "replace(abracadabra, .*?, $1) raises an error, because the pattern matches the zero-length string" in {

      val df = List(
        "abracadabra"
      ).toDF("text")

      val caught = intercept[IndexOutOfBoundsException] {
        df.select(Func.replace(df("text"), ".*?", "$1", "")).collect
      }

      caught.getMessage shouldEqual "No group 1"
    }

    "replace(AAAA, A+, b) returns b" in {

      val df = List("AAAA").toDF("text")

      val result = df.select(Func.replace(df("text"), "A+", "b", "")).collect

      result shouldEqual Array(
        Row("b")
      )
    }

    "replace(AAAA, A+?, b) returns bbbb" in {

      val df = List(
        "AAAA"
      ).toDF("text")

      val result = df.select(Func.replace(df("text"), "A+?", "b", "")).collect

      result shouldEqual Array(
        Row("bbbb")
      )
    }

    "replace(darted, ^(.*?)d(.*)$, $1c$2) returns carted. (The first d is replaced.)" in {

      val df = List(
        "darted"
      ).toDF("text")

      val result =
        df.select(Func.replace(df("text"), "^(.*?)d(.*)$", "$1c$2", "")).collect

      result shouldEqual Array(
        Row("carted")
      )
    }

    "replace when pattern occurs with flags" in {

      val df = List(
        "abcd",
        "abaB",
        "bbBB",
        "aaaa"
      ).toDF("text")

      val result = df.select(Func.replace(df("text"), "b", "Z", "i")).collect

      result shouldEqual Array(
        Row("aZcd"),
        Row("aZaZ"),
        Row("ZZZZ"),
        Row("aaaa")
      )
    }
  }

  "Func.strafter" should {

    "find the correct string if it exists" in {

      val df = List(
        "hello#potato",
        "goodbye#tomato"
      ).toDF("text")

      df.select(Func.strafter(df("text"), "#").as("result"))
        .collect shouldEqual Array(
        Row("potato"),
        Row("tomato")
      )
    }

    "return empty strings otherwise" in {

      val df = List(
        "hello potato",
        "goodbye tomato"
      ).toDF("text")

      df.select(Func.strafter(df("text"), "#").as("result"))
        .collect shouldEqual Array(
        Row(""),
        Row("")
      )
    }

    // See: https://www.w3.org/TR/sparql11-query/#func-strafter
    "ww3c test" in {

      val cases = List(
        ("abc", "b", "c"),
        ("\"abc\"@en", "ab", "\"c\"@en"),
        ("\"abc\"@en", "\"b\"@cy", null),
        ("\"abc\"^^xsd:string", "", "\"abc\"^^xsd:string"),
        ("\"abc\"^^xsd:string", "\"a\"^^xsd:other", null),
        ("\"abc\"^^xsd:string", "\"\"^^xsd:string", "\"abc\"^^xsd:string"),
        ("\"abc\"^^xsd:string", "\"z\"^^xsd:string", ""),
        ("abc", "xyz", ""),
        ("\"abc\"@en", "\"z\"@en", ""),
        ("\"abc\"@en", "z", ""),
        ("\"abc\"@en", "\"\"@en", "\"abc\"@en"),
        ("\"abc\"@en", "", "\"abc\"@en")
      )

      cases.map { case (arg1, arg2, expect) =>
        val df       = List(arg1).toDF("arg1")
        val strafter = Func.strafter(df("arg1"), arg2)
        val result = df
          .select(strafter)
          .as("result")
          .collect()

        result shouldEqual Array(Row(expect))
      }
    }
  }

  "Func.strbefore" should {

    "find the correct string if it exists" in {

      val df = List(
        "hello potato",
        "goodbye tomato"
      ).toDF("text")

      df.select(Func.strbefore(df("text"), " ").as("result"))
        .collect shouldEqual Array(
        Row("hello"),
        Row("goodbye")
      )
    }

    "return empty strings otherwise" in {

      val df = List(
        "hello potato",
        "goodbye tomato"
      ).toDF("text")

      df.select(Func.strbefore(df("text"), "#").as("result"))
        .collect shouldEqual Array(
        Row(""),
        Row("")
      )
    }
  }

  "Func.iri" should {

    "do nothing for IRIs" in {

      val df = List(
        "http://google.com",
        "http://other.com"
      ).toDF("text")

      df.select(Func.iri(df("text")).as("result")).collect shouldEqual Array(
        Row("<http://google.com>"),
        Row("<http://other.com>")
      )
    }
  }

  "Func.strends" should {

    "return true if a field ends with a given string" in {

      val df = List(
        "sports car",
        "sedan car"
      ).toDF("text")

      df.select(Func.strends(df("text"), "car").as("result"))
        .collect shouldEqual Array(
        Row(true),
        Row(true)
      )
    }

    "return false otherwise" in {

      val df = List(
        "hello world",
        "hello universe"
      ).toDF("text")

      df.select(Func.strends(df("text"), "dses").as("result"))
        .collect shouldEqual Array(
        Row(false),
        Row(false)
      )
    }
  }

  "Func.strstarts" should {

    "return true if a field starts with a given string" in {

      val df = List(
        "hello world",
        "hello universe"
      ).toDF("text")

      df.select(Func.strstarts(df("text"), "hello").as("result"))
        .collect shouldEqual Array(
        Row(true),
        Row(true)
      )
    }

    "return false otherwise" in {

      val df = List(
        "hello world",
        "hello universe"
      ).toDF("text")

      df.select(Func.strstarts(df("text"), "help").as("result"))
        .collect shouldEqual Array(
        Row(false),
        Row(false)
      )
    }
  }

  "Func.lcase" should {

    "convert all lexical characters to lower case" in {

      val df = List(
        "HELLO",
        "\"HELLO\"@en",
        "\"HELLO\"^^xsd:string"
      ).toDF("text")

      df.select(Func.lcase(df("text")).as("result")).collect shouldEqual Array(
        Row("hello"),
        Row("\"hello\"@en"),
        Row("\"hello\"^^xsd:string")
      )
    }
  }

  "Func.ucase" should {

    "convert all lexical characters to upper case" in {

      val df = List(
        "hello",
        "\"hello\"@en",
        "\"hello\"^^xsd:string"
      ).toDF("text")

      df.select(Func.ucase(df("text")).as("result")).collect shouldEqual Array(
        Row("HELLO"),
        Row("\"HELLO\"@en"),
        Row("\"HELLO\"^^xsd:string")
      )
    }
  }

  "Func.strdt" should {

    "return a literal with lexical for and type specified" in {

      val df = List(
        "123"
      ).toDF("s")

      val result = df
        .select(
          Func.strdt(df("s"), "<http://www.w3.org/2001/XMLSchema#integer>")
        )
        .collect

      result shouldEqual Array(
        Row("\"123\"^^<http://www.w3.org/2001/XMLSchema#integer>")
      )
    }
  }

  "Func.regex" should {

    "return true if a field matches the given regex pattern" in {

      val df = List(
        "Alice",
        "Alison"
      ).toDF("text")

      df.select(Func.regex(df("text"), "^ali", "i").as("result"))
        .collect shouldEqual Array(
        Row(true),
        Row(true)
      )
    }

    "return false otherwise" in {

      val df = List(
        "Alice",
        "Alison"
      ).toDF("text")

      df.select(Func.regex(df("text"), "^ali", "").as("result"))
        .collect shouldEqual Array(
        Row(false),
        Row(false)
      )
    }
  }

  "Func.concat" should {

    "concatenate two string columns" in {

      val df = List(
        ("Hello", " Dolly"),
        ("Here's a song", " Dolly")
      ).toDF("a", "b")

      df.select(Func.concat(df("a"), List(df("b")).toNel.get).as("verses"))
        .collect shouldEqual Array(
        Row("Hello Dolly"),
        Row("Here's a song Dolly")
      )
    }

    "concatenate two string columns with quotes" in {

      val df = List(
        ("\"Hello\"", "\" Dolly\""),
        ("\"Hello\"", " Dolly"),
        ("Hello", "\" Dolly\""),
        ("Hello", " Dolly")
      ).toDF("a", "b")

      df.select(Func.concat(df("a"), List(df("b")).toNel.get).as("verses"))
        .collect shouldEqual Array(
        Row("Hello Dolly"),
        Row("Hello Dolly"),
        Row("Hello Dolly"),
        Row("Hello Dolly")
      )
    }

    "concatenate a column in quotes with a literal string" in {

      val df = List(
        ("Hello", " Dolly"),
        ("Here's a song", " Dolly")
      ).toDF("a", "b")

      df.select(
        Func.concat(df("a"), List(lit(" world!")).toNel.get).as("sentences")
      ).collect shouldEqual Array(
        Row("Hello world!"),
        Row("Here's a song world!")
      )
    }

    "concatenate a column with a literal string in quotes" in {

      val df = List(
        ("\"Hello\"", " Dolly"),
        ("Here's a song", " Dolly")
      ).toDF("a", "b")

      df.select(
        Func.concat(df("a"), List(lit(" world!")).toNel.get).as("sentences")
      ).collect shouldEqual Array(
        Row("Hello world!"),
        Row("Here's a song world!")
      )
    }

    "concatenate a literal string with a column" in {

      val df = List(
        ("Hello", " Dolly"),
        ("Here's a song", " Dolly")
      ).toDF("a", "b")

      df.select(Func.concat(lit("Ciao"), List(df("b")).toNel.get).as("verses"))
        .collect shouldEqual Array(
        Row("Ciao Dolly"),
        Row("Ciao Dolly")
      )
    }

    "concatenate a literal string with a column in quotes" in {

      val df = List(
        ("Hello", "\" Dolly\""),
        ("Here's a song", " Dolly")
      ).toDF("a", "b")

      df.select(Func.concat(lit("Ciao"), List(df("b")).toNel.get).as("verses"))
        .collect shouldEqual Array(
        Row("Ciao Dolly"),
        Row("Ciao Dolly")
      )
    }

    "concatenate mixing literals and string columns multiple times" in {

      val df = List(
        ("Hello", "\" Dolly\""),
        ("Here's a song", " Dolly")
      ).toDF("a", "b")

      df.select(Func.concat(lit("Ciao"), List(df("b")).toNel.get).as("verses"))
        .collect shouldEqual Array(
        Row("Ciao Dolly"),
        Row("Ciao Dolly")
      )
    }

    "www3c tests" in {

      val cases = List(
        ("foo", "bar", "foobar"),
        ("\"foo\"@en", "\"bar\"@en", "\"foobar\"@en"),
        (
          "\"foo\"^^xsd:string",
          "\"bar\"^^xsd:string",
          "\"foobar\"^^xsd:string"
        ),
        ("foo", "\"bar\"^^xsd:string", "foobar"),
        ("\"foo\"@en", "bar", "foobar"),
        ("\"foo\"@en", "\"bar\"^^xsd:string", "foobar")
      )

      cases.map { case (arg1, arg2, expected) =>
        val df     = List(arg1).toDF("arg1")
        val concat = Func.concat(df("arg1"), List(lit(arg2)).toNel.get)
        val result =
          df.select(concat).as("result").collect()
        result shouldEqual Array(Row(expected))
      }
    }
  }

  "Func.strlen" should {

    "count characters on plain string" in {
      val df = List(
        "chat"
      ).toDF("a")

      df.select(Func.strlen(df("a"))).collect shouldEqual Array(
        Row(4)
      )
    }

    "count characters on typed string" in {
      val df = List(
        "\"chat\"^^xsd:string"
      ).toDF("a")

      df.select(Func.strlen(df("a"))).collect shouldEqual Array(
        Row(4)
      )
    }

    "count characters on localized string" in {
      val df = List(
        "\"chat\"@en"
      ).toDF("a")

      df.select(Func.strlen(df("a"))).collect shouldEqual Array(
        Row(4)
      )
    }
  }

  "Func.equals" should {

    "operates on numbers correctly" when {

      "first argument is not typed number and second is other number type" in {
        val plainNumberCorrectCases = List(
          ("1", "1", true),
          ("1", "\"1\"^^xsd:int", true),
          ("1", "\"1\"^^xsd:integer", true),
          ("1", "\"1\"^^xsd:decimal", true),
          ("1.23", "\"1.23\"^^xsd:float", true),
          ("1.23", "\"1.23\"^^xsd:double", true),
          ("1.23", "\"1.23\"^^xsd:numeric", true)
        )

        val plainNumberWrongCases = List(
          ("1", "2", false),
          ("1", "\"2\"^^xsd:int", false),
          ("1", "\"2\"^^xsd:integer", false),
          ("1", "\"0\"^^xsd:decimal", false),
          ("1.23", "\"1.1\"^^xsd:float", false),
          ("1.23", "\"1.1\"^^xsd:double", false),
          ("1.23", "\"1.1\"^^xsd:numeric", false)
        )

        val df = (plainNumberCorrectCases ++ plainNumberWrongCases).toDF(
          "arg1",
          "arg2",
          "expected"
        )

        val result   = df.select(Func.equals(df("arg1"), df("arg2")))
        val expected = df.select(col("expected"))

        result.collect() shouldEqual expected.collect()
      }

      "first argument is INT type number and second is other number type" in {

        val intCorrectCases = List(
          ("\"1\"^^xsd:int", "1", true),
          ("\"1\"^^xsd:int", "1.0", true),
          ("\"1\"^^xsd:int", "\"1\"^^xsd:int", true),
          ("\"1\"^^xsd:int", "\"1\"^^xsd:integer", true),
          ("\"1\"^^xsd:int", "\"1\"^^xsd:decimal", true),
          ("\"1\"^^xsd:int", "\"1.0\"^^xsd:float", true),
          ("\"1\"^^xsd:int", "\"1.0\"^^xsd:double", true),
          ("\"1\"^^xsd:int", "\"1.0\"^^xsd:numeric", true)
        )

        val intWrongCases = List(
          ("\"1\"^^xsd:int", "2", false),
          ("\"1\"^^xsd:int", "1.1", false),
          ("\"1\"^^xsd:int", "\"2\"^^xsd:int", false),
          ("\"1\"^^xsd:int", "\"2\"^^xsd:integer", false),
          ("\"1\"^^xsd:int", "\"0\"^^xsd:decimal", false),
          ("\"1\"^^xsd:int", "\"1.1\"^^xsd:float", false),
          ("\"1\"^^xsd:int", "\"1.1\"^^xsd:double", false),
          ("\"1\"^^xsd:int", "\"1.1\"^^xsd:numeric", false)
        )

        val df = (intCorrectCases ++ intWrongCases).toDF(
          "arg1",
          "arg2",
          "expected"
        )

        val result   = df.select(Func.equals(df("arg1"), df("arg2")))
        val expected = df.select(col("expected"))

        result.collect() shouldEqual expected.collect()
      }

      "first argument is INTEGER type number and second is other number type" in {

        val integerCorrectCases = List(
          ("\"1\"^^xsd:integer", "1", true),
          ("\"1\"^^xsd:integer", "1.0", true),
          ("\"1\"^^xsd:integer", "\"1\"^^xsd:int", true),
          ("\"1\"^^xsd:integer", "\"1\"^^xsd:integer", true),
          ("\"1\"^^xsd:integer", "\"1\"^^xsd:decimal", true),
          ("\"1\"^^xsd:integer", "\"1.0\"^^xsd:float", true),
          ("\"1\"^^xsd:integer", "\"1.0\"^^xsd:double", true),
          ("\"1\"^^xsd:integer", "\"1.0\"^^xsd:numeric", true)
        )

        val integerWrongCases = List(
          ("\"1\"^^xsd:integer", "2", false),
          ("\"1\"^^xsd:integer", "1.1", false),
          ("\"1\"^^xsd:integer", "\"2\"^^xsd:int", false),
          ("\"1\"^^xsd:integer", "\"2\"^^xsd:integer", false),
          ("\"1\"^^xsd:integer", "\"0\"^^xsd:decimal", false),
          ("\"1\"^^xsd:integer", "\"1.1\"^^xsd:float", false),
          ("\"1\"^^xsd:integer", "\"1.1\"^^xsd:double", false),
          ("\"1\"^^xsd:integer", "\"1.1\"^^xsd:numeric", false)
        )

        val df = (integerCorrectCases ++ integerWrongCases).toDF(
          "arg1",
          "arg2",
          "expected"
        )

        val result   = df.select(Func.equals(df("arg1"), df("arg2")))
        val expected = df.select(col("expected"))

        result.collect() shouldEqual expected.collect()
      }

      "first argument is DECIMAL type number and second is other number type" in {

        val decimalCorrectCases = List(
          ("\"1\"^^xsd:decimal", "1", true),
          ("\"1\"^^xsd:decimal", "1.0", true),
          ("\"1\"^^xsd:decimal", "\"1\"^^xsd:int", true),
          ("\"1\"^^xsd:decimal", "\"1\"^^xsd:integer", true),
          ("\"1\"^^xsd:decimal", "\"1\"^^xsd:decimal", true),
          ("\"1\"^^xsd:decimal", "\"1.0\"^^xsd:float", true),
          ("\"1\"^^xsd:decimal", "\"1.0\"^^xsd:double", true),
          ("\"1\"^^xsd:decimal", "\"1.0\"^^xsd:numeric", true)
        )

        val decimalWrongCases = List(
          ("\"1\"^^xsd:decimal", "2", false),
          ("\"1\"^^xsd:decimal", "1.1", false),
          ("\"1\"^^xsd:decimal", "\"2\"^^xsd:int", false),
          ("\"1\"^^xsd:decimal", "\"2\"^^xsd:integer", false),
          ("\"1\"^^xsd:decimal", "\"2\"^^xsd:decimal", false),
          ("\"1\"^^xsd:decimal", "\"1.24\"^^xsd:float", false),
          ("\"1\"^^xsd:decimal", "\"1.24\"^^xsd:double", false),
          ("\"1\"^^xsd:decimal", "\"1.24\"^^xsd:numeric", false)
        )

        val df = (decimalCorrectCases ++ decimalWrongCases).toDF(
          "arg1",
          "arg2",
          "expected"
        )

        val result =
          df.select(Func.equals(df("arg1"), df("arg2")))
        val expected = df.select(col("expected"))

        result.collect() shouldEqual expected.collect()
      }

      "first argument is FLOAT type number and second is other number type" in {

        val floatCorrectCases = List(
          ("\"1.23\"^^xsd:float", "1.23", true),
          ("\"1.0\"^^xsd:float", "\"1\"^^xsd:int", true),
          ("\"1.0\"^^xsd:float", "\"1\"^^xsd:integer", true),
          ("\"1.0\"^^xsd:float", "\"1\"^^xsd:decimal", true),
          ("\"1.23\"^^xsd:float", "\"1.23\"^^xsd:float", true),
          ("\"1.23\"^^xsd:float", "\"1.23\"^^xsd:double", true),
          ("\"1.23\"^^xsd:float", "\"1.23\"^^xsd:numeric", true)
        )

        val floatWrongCases = List(
          ("\"1.23\"^^xsd:float", "1.24", false),
          ("\"1.23\"^^xsd:float", "\"1\"^^xsd:int", false),
          ("\"1.23\"^^xsd:float", "\"1\"^^xsd:integer", false),
          ("\"1.23\"^^xsd:float", "\"1\"^^xsd:decimal", false),
          ("\"1.23\"^^xsd:float", "\"1.24\"^^xsd:float", false),
          ("\"1.23\"^^xsd:float", "\"1.24\"^^xsd:double", false),
          ("\"1.23\"^^xsd:float", "\"1.24\"^^xsd:numeric", false)
        )

        val df = (floatCorrectCases ++ floatWrongCases).toDF(
          "arg1",
          "arg2",
          "expected"
        )

        val result =
          df.select(Func.equals(df("arg1"), df("arg2")))
        val expected = df.select(col("expected"))

        result.collect() shouldEqual expected.collect()
      }

      "first argument is DOUBLE type number and second is other number type" in {

        val doubleCorrectCases = List(
          ("\"1.23\"^^xsd:double", "1.23", true),
          ("\"1.0\"^^xsd:double", "\"1\"^^xsd:int", true),
          ("\"1.0\"^^xsd:double", "\"1\"^^xsd:integer", true),
          ("\"1.23\"^^xsd:double", "\"1.23\"^^xsd:decimal", true),
          ("\"1.23\"^^xsd:double", "\"1.23\"^^xsd:float", true),
          ("\"1.23\"^^xsd:double", "\"1.23\"^^xsd:double", true),
          ("\"1.23\"^^xsd:double", "\"1.23\"^^xsd:numeric", true)
        )

        val doubleWrongCases = List(
          ("\"1.23\"^^xsd:double", "1.24", false),
          ("\"1.23\"^^xsd:double", "\"1\"^^xsd:int", false),
          ("\"1.23\"^^xsd:double", "\"1\"^^xsd:integer", false),
          ("\"1.23\"^^xsd:double", "\"1.24\"^^xsd:decimal", false),
          ("\"1.23\"^^xsd:double", "\"1.24\"^^xsd:float", false),
          ("\"1.23\"^^xsd:double", "\"1.24\"^^xsd:double", false),
          ("\"1.23\"^^xsd:double", "\"1.24\"^^xsd:numeric", false)
        )

        val df = (doubleCorrectCases ++ doubleWrongCases).toDF(
          "arg1",
          "arg2",
          "expected"
        )

        val result =
          df.select(Func.equals(df("arg1"), df("arg2")))
        val expected = df.select(col("expected"))

        result.collect() shouldEqual expected.collect()
      }

      "first argument is NUMERIC type number and second is other number type" in {

        val numericCorrectCases = List(
          ("\"1.23\"^^xsd:numeric", "1.23", true),
          ("\"1.0\"^^xsd:numeric", "\"1\"^^xsd:int", true),
          ("\"1.0\"^^xsd:numeric", "\"1\"^^xsd:integer", true),
          ("\"1.23\"^^xsd:numeric", "\"1.23\"^^xsd:decimal", true),
          ("\"1.23\"^^xsd:numeric", "\"1.23\"^^xsd:float", true),
          ("\"1.23\"^^xsd:numeric", "\"1.23\"^^xsd:double", true),
          ("\"1.23\"^^xsd:numeric", "\"1.23\"^^xsd:numeric", true)
        )

        val numericWrongCases = List(
          ("\"1.23\"^^xsd:numeric", "1.24", false),
          ("\"1.23\"^^xsd:numeric", "\"1\"^^xsd:int", false),
          ("\"1.23\"^^xsd:numeric", "\"1\"^^xsd:integer", false),
          ("\"1.23\"^^xsd:numeric", "\"1.24\"^^xsd:decimal", false),
          ("\"1.23\"^^xsd:numeric", "\"1.24\"^^xsd:float", false),
          ("\"1.23\"^^xsd:numeric", "\"1.24\"^^xsd:double", false),
          ("\"1.23\"^^xsd:numeric", "\"1.24\"^^xsd:numeric", false)
        )

        val df = (numericCorrectCases ++ numericWrongCases).toDF(
          "arg1",
          "arg2",
          "expected"
        )

        val result =
          df.select(Func.equals(df("arg1"), df("arg2")))
        val expected = df.select(col("expected"))

        result.collect() shouldEqual expected.collect()
      }
    }

    "operate on equal dates correctly" in {

      forAll { datetime: LocalDateTime =>
        val df = List(
          (
            toRDFDateTime(datetime),
            toRDFDateTime(datetime)
          )
        ).toDF("a", "b")

        df.select(Func.equals(df("a"), df("b"))).collect() shouldEqual Array(
          Row(true)
        )
      }
    }

    "operate on different dates correctly" in {

      forAll { datetime: LocalDateTime =>
        val df = List(
          (
            toRDFDateTime(datetime.plusSeconds(1)),
            toRDFDateTime(datetime)
          )
        ).toDF("a", "b")

        df.select(Func.equals(df("a"), df("b"))).collect() shouldEqual Array(
          Row(false)
        )
      }
    }
  }

  "Func.gt" should {

    "operates on numbers correctly" when {

      "first argument is not typed number and second is other number type" in {
        val plainNumberCorrectCases = List(
          ("2", "1", true),
          ("2", "\"1\"^^xsd:int", true),
          ("2", "\"1\"^^xsd:integer", true),
          ("2", "\"1\"^^xsd:decimal", true),
          ("1.24", "\"1.23\"^^xsd:float", true),
          ("1.24", "\"1.23\"^^xsd:double", true),
          ("1.24", "\"1.23\"^^xsd:numeric", true)
        )

        val plainNumberWrongCases = List(
          ("1", "2", false),
          ("1", "\"2\"^^xsd:int", false),
          ("1", "\"2\"^^xsd:integer", false),
          ("1", "\"2\"^^xsd:decimal", false),
          ("1.23", "\"1.24\"^^xsd:float", false),
          ("1.23", "\"1.24\"^^xsd:double", false),
          ("1.23", "\"1.24\"^^xsd:numeric", false)
        )

        val df = (plainNumberCorrectCases ++ plainNumberWrongCases).toDF(
          "arg1",
          "arg2",
          "expected"
        )

        val result   = df.select(Func.gt(df("arg1"), df("arg2")))
        val expected = df.select(col("expected"))

        result.collect() shouldEqual expected.collect()
      }

      "first argument is INT type number and second is other number type" in {

        val intCorrectCases = List(
          ("\"2\"^^xsd:int", "1", true),
          ("\"2\"^^xsd:int", "1.0", true),
          ("\"2\"^^xsd:int", "\"1\"^^xsd:int", true),
          ("\"2\"^^xsd:int", "\"1\"^^xsd:integer", true),
          ("\"2\"^^xsd:int", "\"1\"^^xsd:decimal", true),
          ("\"2\"^^xsd:int", "\"1.0\"^^xsd:float", true),
          ("\"2\"^^xsd:int", "\"1.0\"^^xsd:double", true),
          ("\"2\"^^xsd:int", "\"1.0\"^^xsd:numeric", true)
        )

        val intWrongCases = List(
          ("\"1\"^^xsd:int", "2", false),
          ("\"1\"^^xsd:int", "1.1", false),
          ("\"1\"^^xsd:int", "\"2\"^^xsd:int", false),
          ("\"1\"^^xsd:int", "\"2\"^^xsd:integer", false),
          ("\"1\"^^xsd:int", "\"2\"^^xsd:decimal", false),
          ("\"1\"^^xsd:int", "\"1.1\"^^xsd:float", false),
          ("\"1\"^^xsd:int", "\"1.1\"^^xsd:double", false),
          ("\"1\"^^xsd:int", "\"1.1\"^^xsd:numeric", false)
        )

        val df = (intCorrectCases ++ intWrongCases).toDF(
          "arg1",
          "arg2",
          "expected"
        )

        val result   = df.select(Func.gt(df("arg1"), df("arg2")))
        val expected = df.select(col("expected"))

        result.collect() shouldEqual expected.collect()
      }

      "first argument is INTEGER type number and second is other number type" in {

        val integerCorrectCases = List(
          ("\"2\"^^xsd:integer", "1", true),
          ("\"2\"^^xsd:integer", "1.0", true),
          ("\"2\"^^xsd:integer", "\"1\"^^xsd:int", true),
          ("\"2\"^^xsd:integer", "\"1\"^^xsd:integer", true),
          ("\"2\"^^xsd:integer", "\"1\"^^xsd:decimal", true),
          ("\"2\"^^xsd:integer", "\"1.0\"^^xsd:float", true),
          ("\"2\"^^xsd:integer", "\"1.0\"^^xsd:double", true),
          ("\"2\"^^xsd:integer", "\"1.0\"^^xsd:numeric", true)
        )

        val integerWrongCases = List(
          ("\"1\"^^xsd:integer", "2", false),
          ("\"1\"^^xsd:integer", "1.1", false),
          ("\"1\"^^xsd:integer", "\"2\"^^xsd:int", false),
          ("\"1\"^^xsd:integer", "\"2\"^^xsd:integer", false),
          ("\"1\"^^xsd:integer", "\"2\"^^xsd:decimal", false),
          ("\"1\"^^xsd:integer", "\"1.1\"^^xsd:float", false),
          ("\"1\"^^xsd:integer", "\"1.1\"^^xsd:double", false),
          ("\"1\"^^xsd:integer", "\"1.1\"^^xsd:numeric", false)
        )

        val df = (integerCorrectCases ++ integerWrongCases).toDF(
          "arg1",
          "arg2",
          "expected"
        )

        val result   = df.select(Func.gt(df("arg1"), df("arg2")))
        val expected = df.select(col("expected"))

        result.collect() shouldEqual expected.collect()
      }

      "first argument is DECIMAL type number and second is other number type" in {

        val decimalCorrectCases = List(
          ("\"2\"^^xsd:decimal", "1", true),
          ("\"2\"^^xsd:decimal", "1.0", true),
          ("\"2\"^^xsd:decimal", "\"1\"^^xsd:int", true),
          ("\"2\"^^xsd:decimal", "\"1\"^^xsd:integer", true),
          ("\"2\"^^xsd:decimal", "\"1\"^^xsd:decimal", true),
          ("\"2\"^^xsd:decimal", "\"1.0\"^^xsd:float", true),
          ("\"2\"^^xsd:decimal", "\"1.0\"^^xsd:double", true),
          ("\"2\"^^xsd:decimal", "\"1.0\"^^xsd:numeric", true)
        )

        val decimalWrongCases = List(
          ("\"1\"^^xsd:decimal", "2", false),
          ("\"1\"^^xsd:decimal", "1.1", false),
          ("\"1\"^^xsd:decimal", "\"2\"^^xsd:int", false),
          ("\"1\"^^xsd:decimal", "\"2\"^^xsd:integer", false),
          ("\"1\"^^xsd:decimal", "\"2\"^^xsd:decimal", false),
          ("\"1\"^^xsd:decimal", "\"1.24\"^^xsd:float", false),
          ("\"1\"^^xsd:decimal", "\"1.24\"^^xsd:double", false),
          ("\"1\"^^xsd:decimal", "\"1.24\"^^xsd:numeric", false)
        )

        val df = (decimalCorrectCases ++ decimalWrongCases).toDF(
          "arg1",
          "arg2",
          "expected"
        )

        val result =
          df.select(Func.gt(df("arg1"), df("arg2")))
        val expected = df.select(col("expected"))

        result.collect() shouldEqual expected.collect()
      }

      "first argument is FLOAT type number and second is other number type" in {

        val floatCorrectCases = List(
          ("\"1.23\"^^xsd:float", "1.22", true),
          ("\"1.1\"^^xsd:float", "\"1\"^^xsd:int", true),
          ("\"1.1\"^^xsd:float", "\"1\"^^xsd:integer", true),
          ("\"1.1\"^^xsd:float", "\"1\"^^xsd:decimal", true),
          ("\"1.23\"^^xsd:float", "\"1.22\"^^xsd:float", true),
          ("\"1.23\"^^xsd:float", "\"1.22\"^^xsd:double", true),
          ("\"1.23\"^^xsd:float", "\"1.22\"^^xsd:numeric", true)
        )

        val floatWrongCases = List(
          ("\"1.23\"^^xsd:float", "1.24", false),
          ("\"1.23\"^^xsd:float", "\"2\"^^xsd:int", false),
          ("\"1.23\"^^xsd:float", "\"2\"^^xsd:integer", false),
          ("\"1.23\"^^xsd:float", "\"2\"^^xsd:decimal", false),
          ("\"1.23\"^^xsd:float", "\"1.24\"^^xsd:float", false),
          ("\"1.23\"^^xsd:float", "\"1.24\"^^xsd:double", false),
          ("\"1.23\"^^xsd:float", "\"1.24\"^^xsd:numeric", false)
        )

        val df = (floatCorrectCases ++ floatWrongCases).toDF(
          "arg1",
          "arg2",
          "expected"
        )

        val result =
          df.select(Func.gt(df("arg1"), df("arg2")))
        val expected = df.select(col("expected"))

        result.collect() shouldEqual expected.collect()
      }

      "first argument is DOUBLE type number and second is other number type" in {

        val doubleCorrectCases = List(
          ("\"1.23\"^^xsd:double", "1.22", true),
          ("\"1.1\"^^xsd:double", "\"1\"^^xsd:int", true),
          ("\"1.1\"^^xsd:double", "\"1\"^^xsd:integer", true),
          ("\"1.23\"^^xsd:double", "\"1.22\"^^xsd:decimal", true),
          ("\"1.23\"^^xsd:double", "\"1.22\"^^xsd:float", true),
          ("\"1.23\"^^xsd:double", "\"1.22\"^^xsd:double", true),
          ("\"1.23\"^^xsd:double", "\"1.22\"^^xsd:numeric", true)
        )

        val doubleWrongCases = List(
          ("\"1.23\"^^xsd:double", "1.24", false),
          ("\"1.23\"^^xsd:double", "\"2\"^^xsd:int", false),
          ("\"1.23\"^^xsd:double", "\"2\"^^xsd:integer", false),
          ("\"1.23\"^^xsd:double", "\"1.24\"^^xsd:decimal", false),
          ("\"1.23\"^^xsd:double", "\"1.24\"^^xsd:float", false),
          ("\"1.23\"^^xsd:double", "\"1.24\"^^xsd:double", false),
          ("\"1.23\"^^xsd:double", "\"1.24\"^^xsd:numeric", false)
        )

        val df = (doubleCorrectCases ++ doubleWrongCases).toDF(
          "arg1",
          "arg2",
          "expected"
        )

        val result =
          df.select(Func.gt(df("arg1"), df("arg2")))
        val expected = df.select(col("expected"))

        result.collect() shouldEqual expected.collect()
      }

      "first argument is NUMERIC type number and second is other number type" in {

        val numericCorrectCases = List(
          ("\"1.23\"^^xsd:numeric", "1.22", true),
          ("\"1.23\"^^xsd:numeric", "\"1\"^^xsd:int", true),
          ("\"1.23\"^^xsd:numeric", "\"1\"^^xsd:integer", true),
          ("\"1.23\"^^xsd:numeric", "\"1.22\"^^xsd:decimal", true),
          ("\"1.23\"^^xsd:numeric", "\"1.22\"^^xsd:float", true),
          ("\"1.23\"^^xsd:numeric", "\"1.22\"^^xsd:double", true),
          ("\"1.23\"^^xsd:numeric", "\"1.22\"^^xsd:numeric", true)
        )

        val numericWrongCases = List(
          ("\"1.23\"^^xsd:numeric", "1.24", false),
          ("\"1.23\"^^xsd:numeric", "\"2\"^^xsd:int", false),
          ("\"1.23\"^^xsd:numeric", "\"2\"^^xsd:integer", false),
          ("\"1.23\"^^xsd:numeric", "\"1.24\"^^xsd:decimal", false),
          ("\"1.23\"^^xsd:numeric", "\"1.24\"^^xsd:float", false),
          ("\"1.23\"^^xsd:numeric", "\"1.24\"^^xsd:double", false),
          ("\"1.23\"^^xsd:numeric", "\"1.24\"^^xsd:numeric", false)
        )

        val df = (numericCorrectCases ++ numericWrongCases).toDF(
          "arg1",
          "arg2",
          "expected"
        )

        val result =
          df.select(Func.gt(df("arg1"), df("arg2")))
        val expected = df.select(col("expected"))

        result.collect() shouldEqual expected.collect()
      }
    }

    "work for integer values" in {

      val df = List(
        (2, 1)
      ).toDF("a", "b")

      df.select(Func.gt(df("a"), df("b"))).collect() shouldEqual Array(
        Row(true)
      )
    }

    "work in datetimes without a zone" in {

      forAll { datetime: LocalDateTime =>
        val df = List(
          (
            toRDFDateTime(datetime.plusSeconds(1)),
            toRDFDateTime(datetime)
          )
        ).toDF("a", "b")

        df.select(Func.gt(df("a"), df("b"))).collect() shouldEqual Array(
          Row(true)
        )
      }
    }

    "work in datetimes with zone" in {

      forAll { datetime: LocalDateTime =>
        val df = List(
          (
            toRDFDateTime(OffsetDateTime.of(datetime, ZoneOffset.ofHours(4))),
            toRDFDateTime(OffsetDateTime.of(datetime, ZoneOffset.ofHours(5)))
          )
        ).toDF("a", "b")

        df.select(
          Func.gt(df("a"), df("b"))
        ).collect() shouldEqual Array(
          Row(true)
        )
      }
    }
  }

  "Func.lt" should {

    "operates on numbers correctly" when {

      "first argument is not typed number and second is other number type" in {
        val plainNumberCorrectCases = List(
          ("1", "2", true),
          ("1", "\"2\"^^xsd:int", true),
          ("1", "\"2\"^^xsd:integer", true),
          ("1", "\"2\"^^xsd:decimal", true),
          ("1.23", "\"1.24\"^^xsd:float", true),
          ("1.23", "\"1.24\"^^xsd:double", true),
          ("1.23", "\"1.24\"^^xsd:numeric", true)
        )

        val plainNumberWrongCases = List(
          ("2", "1", false),
          ("2", "\"1\"^^xsd:int", false),
          ("2", "\"1\"^^xsd:integer", false),
          ("2", "\"1\"^^xsd:decimal", false),
          ("1.24", "\"1.23\"^^xsd:float", false),
          ("1.24", "\"1.23\"^^xsd:double", false),
          ("1.24", "\"1.23\"^^xsd:numeric", false)
        )

        val df = (plainNumberCorrectCases ++ plainNumberWrongCases).toDF(
          "arg1",
          "arg2",
          "expected"
        )

        val result   = df.select(Func.lt(df("arg1"), df("arg2")))
        val expected = df.select(col("expected"))

        result.collect() shouldEqual expected.collect()
      }

      "first argument is INT type number and second is other number type" in {

        val intCorrectCases = List(
          ("\"1\"^^xsd:int", "2", true),
          ("\"1\"^^xsd:int", "1.1", true),
          ("\"1\"^^xsd:int", "\"2\"^^xsd:int", true),
          ("\"1\"^^xsd:int", "\"2\"^^xsd:integer", true),
          ("\"1\"^^xsd:int", "\"2\"^^xsd:decimal", true),
          ("\"1\"^^xsd:int", "\"1.1\"^^xsd:float", true),
          ("\"1\"^^xsd:int", "\"1.1\"^^xsd:double", true),
          ("\"1\"^^xsd:int", "\"1.1\"^^xsd:numeric", true)
        )

        val intWrongCases = List(
          ("\"2\"^^xsd:int", "1", false),
          ("\"2\"^^xsd:int", "1.1", false),
          ("\"2\"^^xsd:int", "\"1\"^^xsd:int", false),
          ("\"2\"^^xsd:int", "\"1\"^^xsd:integer", false),
          ("\"2\"^^xsd:int", "\"1\"^^xsd:decimal", false),
          ("\"2\"^^xsd:int", "\"1.1\"^^xsd:float", false),
          ("\"2\"^^xsd:int", "\"1.1\"^^xsd:double", false),
          ("\"2\"^^xsd:int", "\"1.1\"^^xsd:numeric", false)
        )

        val df = (intCorrectCases ++ intWrongCases).toDF(
          "arg1",
          "arg2",
          "expected"
        )

        val result   = df.select(Func.lt(df("arg1"), df("arg2")))
        val expected = df.select(col("expected"))

        result.collect() shouldEqual expected.collect()
      }

      "first argument is INTEGER type number and second is other number type" in {

        val integerCorrectCases = List(
          ("\"1\"^^xsd:integer", "2", true),
          ("\"1\"^^xsd:integer", "1.1", true),
          ("\"1\"^^xsd:integer", "\"2\"^^xsd:int", true),
          ("\"1\"^^xsd:integer", "\"2\"^^xsd:integer", true),
          ("\"1\"^^xsd:integer", "\"2\"^^xsd:decimal", true),
          ("\"1\"^^xsd:integer", "\"1.1\"^^xsd:float", true),
          ("\"1\"^^xsd:integer", "\"1.1\"^^xsd:double", true),
          ("\"1\"^^xsd:integer", "\"1.1\"^^xsd:numeric", true)
        )

        val integerWrongCases = List(
          ("\"2\"^^xsd:integer", "1", false),
          ("\"2\"^^xsd:integer", "1.1", false),
          ("\"2\"^^xsd:integer", "\"1\"^^xsd:int", false),
          ("\"2\"^^xsd:integer", "\"1\"^^xsd:integer", false),
          ("\"2\"^^xsd:integer", "\"1\"^^xsd:decimal", false),
          ("\"2\"^^xsd:integer", "\"1.1\"^^xsd:float", false),
          ("\"2\"^^xsd:integer", "\"1.1\"^^xsd:double", false),
          ("\"2\"^^xsd:integer", "\"1.1\"^^xsd:numeric", false)
        )

        val df = (integerCorrectCases ++ integerWrongCases).toDF(
          "arg1",
          "arg2",
          "expected"
        )

        val result   = df.select(Func.lt(df("arg1"), df("arg2")))
        val expected = df.select(col("expected"))

        result.collect() shouldEqual expected.collect()
      }

      "first argument is DECIMAL type number and second is other number type" in {

        val decimalCorrectCases = List(
          ("\"1\"^^xsd:decimal", "2", true),
          ("\"1\"^^xsd:decimal", "1.1", true),
          ("\"1\"^^xsd:decimal", "\"2\"^^xsd:int", true),
          ("\"1\"^^xsd:decimal", "\"2\"^^xsd:integer", true),
          ("\"1\"^^xsd:decimal", "\"2\"^^xsd:decimal", true),
          ("\"1\"^^xsd:decimal", "\"1.1\"^^xsd:float", true),
          ("\"1\"^^xsd:decimal", "\"1.1\"^^xsd:double", true),
          ("\"1\"^^xsd:decimal", "\"1.1\"^^xsd:numeric", true)
        )

        val decimalWrongCases = List(
          ("\"2\"^^xsd:decimal", "1", false),
          ("\"2\"^^xsd:decimal", "1.1", false),
          ("\"2\"^^xsd:decimal", "\"1\"^^xsd:int", false),
          ("\"2\"^^xsd:decimal", "\"1\"^^xsd:integer", false),
          ("\"2\"^^xsd:decimal", "\"1\"^^xsd:decimal", false),
          ("\"2\"^^xsd:decimal", "\"1.24\"^^xsd:float", false),
          ("\"2\"^^xsd:decimal", "\"1.24\"^^xsd:double", false),
          ("\"2\"^^xsd:decimal", "\"1.24\"^^xsd:numeric", false)
        )

        val df = (decimalCorrectCases ++ decimalWrongCases).toDF(
          "arg1",
          "arg2",
          "expected"
        )

        val result =
          df.select(Func.lt(df("arg1"), df("arg2")))
        val expected = df.select(col("expected"))

        result.collect() shouldEqual expected.collect()
      }

      "first argument is FLOAT type number and second is other number type" in {

        val floatCorrectCases = List(
          ("\"1.23\"^^xsd:float", "1.24", true),
          ("\"1.1\"^^xsd:float", "\"2\"^^xsd:int", true),
          ("\"1.1\"^^xsd:float", "\"2\"^^xsd:integer", true),
          ("\"1.1\"^^xsd:float", "\"2\"^^xsd:decimal", true),
          ("\"1.23\"^^xsd:float", "\"1.24\"^^xsd:float", true),
          ("\"1.23\"^^xsd:float", "\"1.24\"^^xsd:double", true),
          ("\"1.23\"^^xsd:float", "\"1.24\"^^xsd:numeric", true)
        )

        val floatWrongCases = List(
          ("\"1.24\"^^xsd:float", "1.23", false),
          ("\"1.24\"^^xsd:float", "\"1\"^^xsd:int", false),
          ("\"1.24\"^^xsd:float", "\"1\"^^xsd:integer", false),
          ("\"1.24\"^^xsd:float", "\"1\"^^xsd:decimal", false),
          ("\"1.24\"^^xsd:float", "\"1.23\"^^xsd:float", false),
          ("\"1.24\"^^xsd:float", "\"1.23\"^^xsd:double", false),
          ("\"1.24\"^^xsd:float", "\"1.23\"^^xsd:numeric", false)
        )

        val df = (floatCorrectCases ++ floatWrongCases).toDF(
          "arg1",
          "arg2",
          "expected"
        )

        val result =
          df.select(Func.lt(df("arg1"), df("arg2")))
        val expected = df.select(col("expected"))

        result.collect() shouldEqual expected.collect()
      }

      "first argument is DOUBLE type number and second is other number type" in {

        val doubleCorrectCases = List(
          ("\"1.23\"^^xsd:double", "1.24", true),
          ("\"1.1\"^^xsd:double", "\"2\"^^xsd:int", true),
          ("\"1.1\"^^xsd:double", "\"2\"^^xsd:integer", true),
          ("\"1.23\"^^xsd:double", "\"1.24\"^^xsd:decimal", true),
          ("\"1.23\"^^xsd:double", "\"1.24\"^^xsd:float", true),
          ("\"1.23\"^^xsd:double", "\"1.24\"^^xsd:double", true),
          ("\"1.23\"^^xsd:double", "\"1.24\"^^xsd:numeric", true)
        )

        val doubleWrongCases = List(
          ("\"1.23\"^^xsd:double", "1.22", false),
          ("\"1.23\"^^xsd:double", "\"1\"^^xsd:int", false),
          ("\"1.23\"^^xsd:double", "\"1\"^^xsd:integer", false),
          ("\"1.23\"^^xsd:double", "\"1.22\"^^xsd:decimal", false),
          ("\"1.23\"^^xsd:double", "\"1.22\"^^xsd:float", false),
          ("\"1.23\"^^xsd:double", "\"1.22\"^^xsd:double", false),
          ("\"1.23\"^^xsd:double", "\"1.22\"^^xsd:numeric", false)
        )

        val df = (doubleCorrectCases ++ doubleWrongCases).toDF(
          "arg1",
          "arg2",
          "expected"
        )

        val result =
          df.select(Func.lt(df("arg1"), df("arg2")))
        val expected = df.select(col("expected"))

        result.collect() shouldEqual expected.collect()
      }

      "first argument is NUMERIC type number and second is other number type" in {

        val numericCorrectCases = List(
          ("\"1.23\"^^xsd:numeric", "1.24", true),
          ("\"1.23\"^^xsd:numeric", "\"2\"^^xsd:int", true),
          ("\"1.23\"^^xsd:numeric", "\"2\"^^xsd:integer", true),
          ("\"1.23\"^^xsd:numeric", "\"1.24\"^^xsd:decimal", true),
          ("\"1.23\"^^xsd:numeric", "\"1.24\"^^xsd:float", true),
          ("\"1.23\"^^xsd:numeric", "\"1.24\"^^xsd:double", true),
          ("\"1.23\"^^xsd:numeric", "\"1.24\"^^xsd:numeric", true)
        )

        val numericWrongCases = List(
          ("\"1.23\"^^xsd:numeric", "1.22", false),
          ("\"1.23\"^^xsd:numeric", "\"1\"^^xsd:int", false),
          ("\"1.23\"^^xsd:numeric", "\"1\"^^xsd:integer", false),
          ("\"1.23\"^^xsd:numeric", "\"1.22\"^^xsd:decimal", false),
          ("\"1.23\"^^xsd:numeric", "\"1.22\"^^xsd:float", false),
          ("\"1.23\"^^xsd:numeric", "\"1.22\"^^xsd:double", false),
          ("\"1.23\"^^xsd:numeric", "\"1.22\"^^xsd:numeric", false)
        )

        val df = (numericCorrectCases ++ numericWrongCases).toDF(
          "arg1",
          "arg2",
          "expected"
        )

        val result =
          df.select(Func.lt(df("arg1"), df("arg2")))
        val expected = df.select(col("expected"))

        result.collect() shouldEqual expected.collect()
      }
    }

    "work for integer values" in {

      val df = List(
        (1, 2)
      ).toDF("a", "b")

      df.select(Func.lt(df("a"), df("b"))).collect() shouldEqual Array(
        Row(true)
      )
    }

    "work in datetimes without a zone" in {

      forAll { datetime: LocalDateTime =>
        val df = List(
          (
            toRDFDateTime(datetime),
            toRDFDateTime(datetime.plusSeconds(1))
          )
        ).toDF("a", "b")

        df.select(Func.lt(df("a"), df("b"))).collect() shouldEqual Array(
          Row(true)
        )
      }
    }

    "work in datetimes with zone" in {

      forAll { datetime: LocalDateTime =>
        val df = List(
          (
            toRDFDateTime(OffsetDateTime.of(datetime, ZoneOffset.ofHours(5))),
            toRDFDateTime(OffsetDateTime.of(datetime, ZoneOffset.ofHours(4)))
          )
        ).toDF("a", "b")

        df.select(
          Func.lt(df("a"), df("b"))
        ).collect() shouldEqual Array(
          Row(true)
        )
      }
    }
  }

  "Func.gte" should {

    "operates on numbers correctly" when {

      "first argument is not typed number and second is other number type" in {
        val plainNumberCorrectCases = List(
          ("2", "1", true),
          ("2", "\"1\"^^xsd:int", true),
          ("2", "\"1\"^^xsd:integer", true),
          ("2", "\"1\"^^xsd:decimal", true),
          ("1.24", "\"1.23\"^^xsd:float", true),
          ("1.24", "\"1.23\"^^xsd:double", true),
          ("1.24", "\"1.23\"^^xsd:numeric", true)
        )

        val plainNumberWrongCases = List(
          ("1", "2", false),
          ("1", "\"2\"^^xsd:int", false),
          ("1", "\"2\"^^xsd:integer", false),
          ("1", "\"2\"^^xsd:decimal", false),
          ("1.23", "\"1.24\"^^xsd:float", false),
          ("1.23", "\"1.24\"^^xsd:double", false),
          ("1.23", "\"1.24\"^^xsd:numeric", false)
        )

        val df = (plainNumberCorrectCases ++ plainNumberWrongCases).toDF(
          "arg1",
          "arg2",
          "expected"
        )

        val result   = df.select(Func.gte(df("arg1"), df("arg2")))
        val expected = df.select(col("expected"))

        result.collect() shouldEqual expected.collect()
      }

      "first argument is INT type number and second is other number type" in {

        val intCorrectCases = List(
          ("\"2\"^^xsd:int", "1", true),
          ("\"2\"^^xsd:int", "1.0", true),
          ("\"2\"^^xsd:int", "\"1\"^^xsd:int", true),
          ("\"2\"^^xsd:int", "\"1\"^^xsd:integer", true),
          ("\"2\"^^xsd:int", "\"1\"^^xsd:decimal", true),
          ("\"2\"^^xsd:int", "\"1.0\"^^xsd:float", true),
          ("\"2\"^^xsd:int", "\"1.0\"^^xsd:double", true),
          ("\"2\"^^xsd:int", "\"1.0\"^^xsd:numeric", true)
        )

        val intWrongCases = List(
          ("\"1\"^^xsd:int", "2", false),
          ("\"1\"^^xsd:int", "1.1", false),
          ("\"1\"^^xsd:int", "\"2\"^^xsd:int", false),
          ("\"1\"^^xsd:int", "\"2\"^^xsd:integer", false),
          ("\"1\"^^xsd:int", "\"2\"^^xsd:decimal", false),
          ("\"1\"^^xsd:int", "\"1.1\"^^xsd:float", false),
          ("\"1\"^^xsd:int", "\"1.1\"^^xsd:double", false),
          ("\"1\"^^xsd:int", "\"1.1\"^^xsd:numeric", false)
        )

        val df = (intCorrectCases ++ intWrongCases).toDF(
          "arg1",
          "arg2",
          "expected"
        )

        val result   = df.select(Func.gte(df("arg1"), df("arg2")))
        val expected = df.select(col("expected"))

        result.collect() shouldEqual expected.collect()
      }

      "first argument is INTEGER type number and second is other number type" in {

        val integerCorrectCases = List(
          ("\"2\"^^xsd:integer", "1", true),
          ("\"2\"^^xsd:integer", "1.0", true),
          ("\"2\"^^xsd:integer", "\"1\"^^xsd:int", true),
          ("\"2\"^^xsd:integer", "\"1\"^^xsd:integer", true),
          ("\"2\"^^xsd:integer", "\"1\"^^xsd:decimal", true),
          ("\"2\"^^xsd:integer", "\"1.0\"^^xsd:float", true),
          ("\"2\"^^xsd:integer", "\"1.0\"^^xsd:double", true),
          ("\"2\"^^xsd:integer", "\"1.0\"^^xsd:numeric", true)
        )

        val integerWrongCases = List(
          ("\"1\"^^xsd:integer", "2", false),
          ("\"1\"^^xsd:integer", "1.1", false),
          ("\"1\"^^xsd:integer", "\"2\"^^xsd:int", false),
          ("\"1\"^^xsd:integer", "\"2\"^^xsd:integer", false),
          ("\"1\"^^xsd:integer", "\"2\"^^xsd:decimal", false),
          ("\"1\"^^xsd:integer", "\"1.1\"^^xsd:float", false),
          ("\"1\"^^xsd:integer", "\"1.1\"^^xsd:double", false),
          ("\"1\"^^xsd:integer", "\"1.1\"^^xsd:numeric", false)
        )

        val df = (integerCorrectCases ++ integerWrongCases).toDF(
          "arg1",
          "arg2",
          "expected"
        )

        val result   = df.select(Func.gte(df("arg1"), df("arg2")))
        val expected = df.select(col("expected"))

        result.collect() shouldEqual expected.collect()
      }

      "first argument is DECIMAL type number and second is other number type" in {

        val decimalCorrectCases = List(
          ("\"2\"^^xsd:decimal", "1", true),
          ("\"2\"^^xsd:decimal", "1.0", true),
          ("\"2\"^^xsd:decimal", "\"1\"^^xsd:int", true),
          ("\"2\"^^xsd:decimal", "\"1\"^^xsd:integer", true),
          ("\"2\"^^xsd:decimal", "\"1\"^^xsd:decimal", true),
          ("\"2\"^^xsd:decimal", "\"1.0\"^^xsd:float", true),
          ("\"2\"^^xsd:decimal", "\"1.0\"^^xsd:double", true),
          ("\"2\"^^xsd:decimal", "\"1.0\"^^xsd:numeric", true)
        )

        val decimalWrongCases = List(
          ("\"1\"^^xsd:decimal", "2", false),
          ("\"1\"^^xsd:decimal", "1.1", false),
          ("\"1\"^^xsd:decimal", "\"2\"^^xsd:int", false),
          ("\"1\"^^xsd:decimal", "\"2\"^^xsd:integer", false),
          ("\"1\"^^xsd:decimal", "\"2\"^^xsd:decimal", false),
          ("\"1\"^^xsd:decimal", "\"1.24\"^^xsd:float", false),
          ("\"1\"^^xsd:decimal", "\"1.24\"^^xsd:double", false),
          ("\"1\"^^xsd:decimal", "\"1.24\"^^xsd:numeric", false)
        )

        val df = (decimalCorrectCases ++ decimalWrongCases).toDF(
          "arg1",
          "arg2",
          "expected"
        )

        val result =
          df.select(Func.gte(df("arg1"), df("arg2")))
        val expected = df.select(col("expected"))

        result.collect() shouldEqual expected.collect()
      }

      "first argument is FLOAT type number and second is other number type" in {

        val floatCorrectCases = List(
          ("\"1.23\"^^xsd:float", "1.22", true),
          ("\"1.1\"^^xsd:float", "\"1\"^^xsd:int", true),
          ("\"1.1\"^^xsd:float", "\"1\"^^xsd:integer", true),
          ("\"1.1\"^^xsd:float", "\"1\"^^xsd:decimal", true),
          ("\"1.23\"^^xsd:float", "\"1.22\"^^xsd:float", true),
          ("\"1.23\"^^xsd:float", "\"1.22\"^^xsd:double", true),
          ("\"1.23\"^^xsd:float", "\"1.22\"^^xsd:numeric", true)
        )

        val floatWrongCases = List(
          ("\"1.23\"^^xsd:float", "1.24", false),
          ("\"1.23\"^^xsd:float", "\"2\"^^xsd:int", false),
          ("\"1.23\"^^xsd:float", "\"2\"^^xsd:integer", false),
          ("\"1.23\"^^xsd:float", "\"2\"^^xsd:decimal", false),
          ("\"1.23\"^^xsd:float", "\"1.24\"^^xsd:float", false),
          ("\"1.23\"^^xsd:float", "\"1.24\"^^xsd:double", false),
          ("\"1.23\"^^xsd:float", "\"1.24\"^^xsd:numeric", false)
        )

        val df = (floatCorrectCases ++ floatWrongCases).toDF(
          "arg1",
          "arg2",
          "expected"
        )

        val result =
          df.select(Func.gte(df("arg1"), df("arg2")))
        val expected = df.select(col("expected"))

        result.collect() shouldEqual expected.collect()
      }

      "first argument is DOUBLE type number and second is other number type" in {

        val doubleCorrectCases = List(
          ("\"1.23\"^^xsd:double", "1.22", true),
          ("\"1.1\"^^xsd:double", "\"1\"^^xsd:int", true),
          ("\"1.1\"^^xsd:double", "\"1\"^^xsd:integer", true),
          ("\"1.23\"^^xsd:double", "\"1.22\"^^xsd:decimal", true),
          ("\"1.23\"^^xsd:double", "\"1.22\"^^xsd:float", true),
          ("\"1.23\"^^xsd:double", "\"1.22\"^^xsd:double", true),
          ("\"1.23\"^^xsd:double", "\"1.22\"^^xsd:numeric", true)
        )

        val doubleWrongCases = List(
          ("\"1.23\"^^xsd:double", "1.24", false),
          ("\"1.23\"^^xsd:double", "\"2\"^^xsd:int", false),
          ("\"1.23\"^^xsd:double", "\"2\"^^xsd:integer", false),
          ("\"1.23\"^^xsd:double", "\"1.24\"^^xsd:decimal", false),
          ("\"1.23\"^^xsd:double", "\"1.24\"^^xsd:float", false),
          ("\"1.23\"^^xsd:double", "\"1.24\"^^xsd:double", false),
          ("\"1.23\"^^xsd:double", "\"1.24\"^^xsd:numeric", false)
        )

        val df = (doubleCorrectCases ++ doubleWrongCases).toDF(
          "arg1",
          "arg2",
          "expected"
        )

        val result =
          df.select(Func.gte(df("arg1"), df("arg2")))
        val expected = df.select(col("expected"))

        result.collect() shouldEqual expected.collect()
      }

      "first argument is NUMERIC type number and second is other number type" in {

        val numericCorrectCases = List(
          ("\"1.23\"^^xsd:numeric", "1.22", true),
          ("\"1.23\"^^xsd:numeric", "\"1\"^^xsd:int", true),
          ("\"1.23\"^^xsd:numeric", "\"1\"^^xsd:integer", true),
          ("\"1.23\"^^xsd:numeric", "\"1.22\"^^xsd:decimal", true),
          ("\"1.23\"^^xsd:numeric", "\"1.22\"^^xsd:float", true),
          ("\"1.23\"^^xsd:numeric", "\"1.22\"^^xsd:double", true),
          ("\"1.23\"^^xsd:numeric", "\"1.22\"^^xsd:numeric", true)
        )

        val numericWrongCases = List(
          ("\"1.23\"^^xsd:numeric", "1.24", false),
          ("\"1.23\"^^xsd:numeric", "\"2\"^^xsd:int", false),
          ("\"1.23\"^^xsd:numeric", "\"2\"^^xsd:integer", false),
          ("\"1.23\"^^xsd:numeric", "\"1.24\"^^xsd:decimal", false),
          ("\"1.23\"^^xsd:numeric", "\"1.24\"^^xsd:float", false),
          ("\"1.23\"^^xsd:numeric", "\"1.24\"^^xsd:double", false),
          ("\"1.23\"^^xsd:numeric", "\"1.24\"^^xsd:numeric", false)
        )

        val df = (numericCorrectCases ++ numericWrongCases).toDF(
          "arg1",
          "arg2",
          "expected"
        )

        val result =
          df.select(Func.gte(df("arg1"), df("arg2")))
        val expected = df.select(col("expected"))

        result.collect() shouldEqual expected.collect()
      }
    }

    "work for integer values" in {

      val df = List(
        (2, 1),
        (2, 2)
      ).toDF("a", "b")

      df.select(Func.gte(df("a"), df("b"))).collect() shouldEqual Array(
        Row(true),
        Row(true)
      )
    }

    "work in datetimes without a zone" in {

      forAll { datetime: LocalDateTime =>
        val df = List(
          (
            toRDFDateTime(datetime.plusSeconds(1)),
            toRDFDateTime(datetime)
          )
        ).toDF("a", "b")

        df.select(Func.gte(df("a"), df("b"))).collect() shouldEqual Array(
          Row(true)
        )
      }
    }

    "work in datetimes with zone" in {

      forAll { datetime: LocalDateTime =>
        val df = List(
          (
            toRDFDateTime(OffsetDateTime.of(datetime, ZoneOffset.ofHours(4))),
            toRDFDateTime(OffsetDateTime.of(datetime, ZoneOffset.ofHours(5)))
          )
        ).toDF("a", "b")

        df.select(
          Func.gte(df("a"), df("b"))
        ).collect() shouldEqual Array(
          Row(true)
        )
      }
    }
  }

  "Func.lte" should {

    "operates on numbers correctly" when {

      "first argument is not typed number and second is other number type" in {
        val plainNumberCorrectCases = List(
          ("1", "2", true),
          ("1", "\"2\"^^xsd:int", true),
          ("1", "\"2\"^^xsd:integer", true),
          ("1", "\"2\"^^xsd:decimal", true),
          ("1.23", "\"1.24\"^^xsd:float", true),
          ("1.23", "\"1.24\"^^xsd:double", true),
          ("1.23", "\"1.24\"^^xsd:numeric", true)
        )

        val plainNumberWrongCases = List(
          ("2", "1", false),
          ("2", "\"1\"^^xsd:int", false),
          ("2", "\"1\"^^xsd:integer", false),
          ("2", "\"1\"^^xsd:decimal", false),
          ("1.24", "\"1.23\"^^xsd:float", false),
          ("1.24", "\"1.23\"^^xsd:double", false),
          ("1.24", "\"1.23\"^^xsd:numeric", false)
        )

        val df = (plainNumberCorrectCases ++ plainNumberWrongCases).toDF(
          "arg1",
          "arg2",
          "expected"
        )

        val result   = df.select(Func.lte(df("arg1"), df("arg2")))
        val expected = df.select(col("expected"))

        result.collect() shouldEqual expected.collect()
      }

      "first argument is INT type number and second is other number type" in {

        val intCorrectCases = List(
          ("\"1\"^^xsd:int", "2", true),
          ("\"1\"^^xsd:int", "1.1", true),
          ("\"1\"^^xsd:int", "\"2\"^^xsd:int", true),
          ("\"1\"^^xsd:int", "\"2\"^^xsd:integer", true),
          ("\"1\"^^xsd:int", "\"2\"^^xsd:decimal", true),
          ("\"1\"^^xsd:int", "\"1.1\"^^xsd:float", true),
          ("\"1\"^^xsd:int", "\"1.1\"^^xsd:double", true),
          ("\"1\"^^xsd:int", "\"1.1\"^^xsd:numeric", true)
        )

        val intWrongCases = List(
          ("\"2\"^^xsd:int", "1", false),
          ("\"2\"^^xsd:int", "1.1", false),
          ("\"2\"^^xsd:int", "\"1\"^^xsd:int", false),
          ("\"2\"^^xsd:int", "\"1\"^^xsd:integer", false),
          ("\"2\"^^xsd:int", "\"1\"^^xsd:decimal", false),
          ("\"2\"^^xsd:int", "\"1.1\"^^xsd:float", false),
          ("\"2\"^^xsd:int", "\"1.1\"^^xsd:double", false),
          ("\"2\"^^xsd:int", "\"1.1\"^^xsd:numeric", false)
        )

        val df = (intCorrectCases ++ intWrongCases).toDF(
          "arg1",
          "arg2",
          "expected"
        )

        val result   = df.select(Func.lte(df("arg1"), df("arg2")))
        val expected = df.select(col("expected"))

        result.collect() shouldEqual expected.collect()
      }

      "first argument is INTEGER type number and second is other number type" in {

        val integerCorrectCases = List(
          ("\"1\"^^xsd:integer", "2", true),
          ("\"1\"^^xsd:integer", "1.1", true),
          ("\"1\"^^xsd:integer", "\"2\"^^xsd:int", true),
          ("\"1\"^^xsd:integer", "\"2\"^^xsd:integer", true),
          ("\"1\"^^xsd:integer", "\"2\"^^xsd:decimal", true),
          ("\"1\"^^xsd:integer", "\"1.1\"^^xsd:float", true),
          ("\"1\"^^xsd:integer", "\"1.1\"^^xsd:double", true),
          ("\"1\"^^xsd:integer", "\"1.1\"^^xsd:numeric", true)
        )

        val integerWrongCases = List(
          ("\"2\"^^xsd:integer", "1", false),
          ("\"2\"^^xsd:integer", "1.1", false),
          ("\"2\"^^xsd:integer", "\"1\"^^xsd:int", false),
          ("\"2\"^^xsd:integer", "\"1\"^^xsd:integer", false),
          ("\"2\"^^xsd:integer", "\"1\"^^xsd:decimal", false),
          ("\"2\"^^xsd:integer", "\"1.1\"^^xsd:float", false),
          ("\"2\"^^xsd:integer", "\"1.1\"^^xsd:double", false),
          ("\"2\"^^xsd:integer", "\"1.1\"^^xsd:numeric", false)
        )

        val df = (integerCorrectCases ++ integerWrongCases).toDF(
          "arg1",
          "arg2",
          "expected"
        )

        val result   = df.select(Func.lte(df("arg1"), df("arg2")))
        val expected = df.select(col("expected"))

        result.collect() shouldEqual expected.collect()
      }

      "first argument is DECIMAL type number and second is other number type" in {

        val decimalCorrectCases = List(
          ("\"1\"^^xsd:decimal", "2", true),
          ("\"1\"^^xsd:decimal", "1.1", true),
          ("\"1\"^^xsd:decimal", "\"2\"^^xsd:int", true),
          ("\"1\"^^xsd:decimal", "\"2\"^^xsd:integer", true),
          ("\"1\"^^xsd:decimal", "\"2\"^^xsd:decimal", true),
          ("\"1\"^^xsd:decimal", "\"1.1\"^^xsd:float", true),
          ("\"1\"^^xsd:decimal", "\"1.1\"^^xsd:double", true),
          ("\"1\"^^xsd:decimal", "\"1.1\"^^xsd:numeric", true)
        )

        val decimalWrongCases = List(
          ("\"2\"^^xsd:decimal", "1", false),
          ("\"2\"^^xsd:decimal", "1.1", false),
          ("\"2\"^^xsd:decimal", "\"1\"^^xsd:int", false),
          ("\"2\"^^xsd:decimal", "\"1\"^^xsd:integer", false),
          ("\"2\"^^xsd:decimal", "\"1\"^^xsd:decimal", false),
          ("\"2\"^^xsd:decimal", "\"1.24\"^^xsd:float", false),
          ("\"2\"^^xsd:decimal", "\"1.24\"^^xsd:double", false),
          ("\"2\"^^xsd:decimal", "\"1.24\"^^xsd:numeric", false)
        )

        val df = (decimalCorrectCases ++ decimalWrongCases).toDF(
          "arg1",
          "arg2",
          "expected"
        )

        val result =
          df.select(Func.lte(df("arg1"), df("arg2")))
        val expected = df.select(col("expected"))

        result.collect() shouldEqual expected.collect()
      }

      "first argument is FLOAT type number and second is other number type" in {

        val floatCorrectCases = List(
          ("\"1.23\"^^xsd:float", "1.24", true),
          ("\"1.1\"^^xsd:float", "\"2\"^^xsd:int", true),
          ("\"1.1\"^^xsd:float", "\"2\"^^xsd:integer", true),
          ("\"1.1\"^^xsd:float", "\"2\"^^xsd:decimal", true),
          ("\"1.23\"^^xsd:float", "\"1.24\"^^xsd:float", true),
          ("\"1.23\"^^xsd:float", "\"1.24\"^^xsd:double", true),
          ("\"1.23\"^^xsd:float", "\"1.24\"^^xsd:numeric", true)
        )

        val floatWrongCases = List(
          ("\"1.24\"^^xsd:float", "1.23", false),
          ("\"1.24\"^^xsd:float", "\"1\"^^xsd:int", false),
          ("\"1.24\"^^xsd:float", "\"1\"^^xsd:integer", false),
          ("\"1.24\"^^xsd:float", "\"1\"^^xsd:decimal", false),
          ("\"1.24\"^^xsd:float", "\"1.23\"^^xsd:float", false),
          ("\"1.24\"^^xsd:float", "\"1.23\"^^xsd:double", false),
          ("\"1.24\"^^xsd:float", "\"1.23\"^^xsd:numeric", false)
        )

        val df = (floatCorrectCases ++ floatWrongCases).toDF(
          "arg1",
          "arg2",
          "expected"
        )

        val result =
          df.select(Func.lte(df("arg1"), df("arg2")))
        val expected = df.select(col("expected"))

        result.collect() shouldEqual expected.collect()
      }

      "first argument is DOUBLE type number and second is other number type" in {

        val doubleCorrectCases = List(
          ("\"1.23\"^^xsd:double", "1.24", true),
          ("\"1.1\"^^xsd:double", "\"2\"^^xsd:int", true),
          ("\"1.1\"^^xsd:double", "\"2\"^^xsd:integer", true),
          ("\"1.23\"^^xsd:double", "\"1.24\"^^xsd:decimal", true),
          ("\"1.23\"^^xsd:double", "\"1.24\"^^xsd:float", true),
          ("\"1.23\"^^xsd:double", "\"1.24\"^^xsd:double", true),
          ("\"1.23\"^^xsd:double", "\"1.24\"^^xsd:numeric", true)
        )

        val doubleWrongCases = List(
          ("\"1.23\"^^xsd:double", "1.22", false),
          ("\"1.23\"^^xsd:double", "\"1\"^^xsd:int", false),
          ("\"1.23\"^^xsd:double", "\"1\"^^xsd:integer", false),
          ("\"1.23\"^^xsd:double", "\"1.22\"^^xsd:decimal", false),
          ("\"1.23\"^^xsd:double", "\"1.22\"^^xsd:float", false),
          ("\"1.23\"^^xsd:double", "\"1.22\"^^xsd:double", false),
          ("\"1.23\"^^xsd:double", "\"1.22\"^^xsd:numeric", false)
        )

        val df = (doubleCorrectCases ++ doubleWrongCases).toDF(
          "arg1",
          "arg2",
          "expected"
        )

        val result =
          df.select(Func.lte(df("arg1"), df("arg2")))
        val expected = df.select(col("expected"))

        result.collect() shouldEqual expected.collect()
      }

      "first argument is NUMERIC type number and second is other number type" in {

        val numericCorrectCases = List(
          ("\"1.23\"^^xsd:numeric", "1.24", true),
          ("\"1.23\"^^xsd:numeric", "\"2\"^^xsd:int", true),
          ("\"1.23\"^^xsd:numeric", "\"2\"^^xsd:integer", true),
          ("\"1.23\"^^xsd:numeric", "\"1.24\"^^xsd:decimal", true),
          ("\"1.23\"^^xsd:numeric", "\"1.24\"^^xsd:float", true),
          ("\"1.23\"^^xsd:numeric", "\"1.24\"^^xsd:double", true),
          ("\"1.23\"^^xsd:numeric", "\"1.24\"^^xsd:numeric", true)
        )

        val numericWrongCases = List(
          ("\"1.23\"^^xsd:numeric", "1.22", false),
          ("\"1.23\"^^xsd:numeric", "\"1\"^^xsd:int", false),
          ("\"1.23\"^^xsd:numeric", "\"1\"^^xsd:integer", false),
          ("\"1.23\"^^xsd:numeric", "\"1.22\"^^xsd:decimal", false),
          ("\"1.23\"^^xsd:numeric", "\"1.22\"^^xsd:float", false),
          ("\"1.23\"^^xsd:numeric", "\"1.22\"^^xsd:double", false),
          ("\"1.23\"^^xsd:numeric", "\"1.22\"^^xsd:numeric", false)
        )

        val df = (numericCorrectCases ++ numericWrongCases).toDF(
          "arg1",
          "arg2",
          "expected"
        )

        val result =
          df.select(Func.lte(df("arg1"), df("arg2")))
        val expected = df.select(col("expected"))

        result.collect() shouldEqual expected.collect()
      }
    }

    "work for integer values" in {

      val df = List(
        (1, 2),
        (2, 2)
      ).toDF("a", "b")

      df.select(Func.lte(df("a"), df("b"))).collect() shouldEqual Array(
        Row(true),
        Row(true)
      )
    }

    "work in datetimes without a zone" in {

      forAll { datetime: LocalDateTime =>
        val df = List(
          (
            toRDFDateTime(datetime),
            toRDFDateTime(datetime.plusSeconds(1))
          )
        ).toDF("a", "b")

        df.select(Func.lte(df("a"), df("b"))).collect() shouldEqual Array(
          Row(true)
        )
      }
    }

    "work in datetimes with zone" in {

      forAll { datetime: LocalDateTime =>
        val df = List(
          (
            toRDFDateTime(OffsetDateTime.of(datetime, ZoneOffset.ofHours(5))),
            toRDFDateTime(OffsetDateTime.of(datetime, ZoneOffset.ofHours(4)))
          )
        ).toDF("a", "b")

        df.select(
          Func.lte(df("a"), df("b"))
        ).collect() shouldEqual Array(
          Row(true)
        )
      }
    }

    "Func.substr" should {

      "correctly return the substring of a given column without length specified" in {

        val df = List(
          "hello world",
          "hello universe"
        ).toDF("text")

        df.select(Func.substr(df("text"), 5, None).as("result"))
          .collect shouldEqual Array(
          Row("o world"),
          Row("o universe")
        )
      }

      "correctly return the substring of a given column with length specified" in {

        val df = List(
          "hello world",
          "hello universe"
        ).toDF("text")

        df.select(Func.substr(df("text"), 5, Some(3)).as("result"))
          .collect shouldEqual Array(
          Row("o w"),
          Row("o u")
        )
      }
    }
  }

  "Func.parseDAteFromRDFDateTime" should {
    "work for all types of dates specified by RDF spec" in {

      val df = List(
        """"2001-10-26T21:32:52"^^xsd:dateTime""",
        """"2001-10-26T21:32:52+02:00"^^xsd:dateTime""",
        """"2001-10-26T19:32:52Z"^^xsd:dateTime""",
        """"2001-10-26T19:32:52+00:00"^^xsd:dateTime""",
        """"2001-10-26T21:32:52.12679"^^xsd:dateTime"""
      ).toDF("date")

      df.select(Func.parseDateFromRDFDateTime(df("date")))
        .collect()
        .map(_.get(0)) shouldNot contain(null)
    }
  }

  "Func.str" should {
    "remove angle brackets from uris" in {

      val initial = List(
        ("<mailto:pepe@examplle.com>", "mailto:pepe@examplle.com"),
        ("<http://example.com>", "http://example.com")
      ).toDF("input", "expected")

      val df = initial.withColumn("result", Func.str(initial("input")))

      df.collect.foreach { case Row(_, expected, result) =>
        expected shouldEqual result
      }
    }

    "don't modify non-uri strings" in {

      val initial = List(
        ("mailto:pepe@examplle.com>", "mailto:pepe@examplle.com>"),
        ("http://example.com>", "http://example.com>"),
        ("hello", "hello"),
        ("\"test\"", "\"test\""),
        ("1", "1")
      ).toDF("input", "expected")

      val df = initial.withColumn("result", Func.str(initial("input")))

      df.collect.foreach { case Row(_, expected, result) =>
        expected shouldEqual result
      }
    }
  }

  "Func.isTypedLiteral" should {
    "identify RDF literals correctly" in {

      val initial = List(
        ("\"1\"^^xsd:int", true),
        ("\"1.1\"^^xsd:decimal", true),
        ("\"1.1\"^^xsd:float", true),
        ("\"1.1\"^^xsd:double", true),
        ("\"1\"", false),
        ("1", false),
        ("false", false)
      ).toDF("input", "expected")

      val df =
        initial.withColumn("result", Func.isTypedLiteral(initial("input")))

      df.collect.foreach { case Row(_, expected, result) =>
        expected shouldEqual result
      }
    }
  }

  "Func.extractNumber" should {
    "extract the numeric part of numeric RDF literals" in {

      val initial = List(
        ("\"1\"^^xsd:int", "1"),
        ("\"1\"^^xsd:integer", "1"),
        ("\"1.1\"^^xsd:decimal", "1.1"),
        ("\"1.1\"^^xsd:float", "1.1"),
        ("\"1.1\"^^xsd:double", "1.1"),
        ("\"1\"^^<http://www.w3.org/2001/XMLSchema#int>", "1"),
        ("\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>", "1"),
        ("\"1.1\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "1.1"),
        ("\"1.1\"^^<http://www.w3.org/2001/XMLSchema#float>", "1.1"),
        ("\"1.1\"^^<http://www.w3.org/2001/XMLSchema#double>", "1.1")
      ).toDF("input", "expected")

      val df =
        initial.withColumn("result", Func.extractNumber(initial("input")))

      df.collect.foreach { case Row(_, expected, result) =>
        expected shouldEqual result
      }
    }

    "return null if the value is not an RDF literal, or is not numeric" in {

      val initial = List(
        ("\"1\"^^xsd:string", null),
        ("\"03-03-2020\"^^xsd:date", null),
        ("1.1", null)
      ).toDF("input", "expected")

      val df =
        initial.withColumn("result", Func.extractNumber(initial("input")))

      df.collect.foreach { case Row(_, expected, result) =>
        expected shouldEqual result
      }
    }
  }

  "Func.tryExtractNumber" should {
    "extract the numeric part of numeric RDF literals" in {

      val initial = List(
        ("\"1\"^^xsd:int", "1"),
        ("\"1\"^^xsd:integer", "1"),
        ("\"1.1\"^^xsd:decimal", "1.1"),
        ("\"1.1\"^^xsd:float", "1.1"),
        ("\"1.1\"^^xsd:double", "1.1"),
        ("\"1\"^^<http://www.w3.org/2001/XMLSchema#int>", "1"),
        ("\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>", "1"),
        ("\"1.1\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "1.1"),
        ("\"1.1\"^^<http://www.w3.org/2001/XMLSchema#float>", "1.1"),
        ("\"1.1\"^^<http://www.w3.org/2001/XMLSchema#double>", "1.1")
      ).toDF("input", "expected")

      val df =
        initial.withColumn("result", Func.tryExtractNumber(initial("input")))

      df.collect.foreach { case Row(_, expected, result) =>
        expected shouldEqual result
      }
    }

    "return the value unchanged if the value is not an RDF literal, or is not numeric" in {

      val initial = List(
        ("\"1\"^^xsd:string", "\"1\"^^xsd:string"),
        ("\"03-03-2020\"^^xsd:date", "\"03-03-2020\"^^xsd:date"),
        ("1.1", "1.1")
      ).toDF("input", "expected")

      val df =
        initial.withColumn("result", Func.tryExtractNumber(initial("input")))

      df.collect.foreach { case Row(_, expected, result) =>
        expected shouldEqual result
      }
    }
  }

  "Func.extractType" should {
    "extract the type from an RDF literal" in {

      val initial = List(
        ("\"1\"^^xsd:int", "xsd:int"),
        ("\"1.1\"^^xsd:decimal", "xsd:decimal"),
        ("\"1.1\"^^xsd:float", "xsd:float"),
        ("\"1.1\"^^xsd:double", "xsd:double")
      ).toDF("input", "expected")

      val df = initial.withColumn("result", Func.extractType(initial("input")))

      df.collect.foreach { case Row(_, expected, result) =>
        expected shouldEqual result
      }
    }
  }

  "Func.isNumeric" should {

    "return true when the term is numeric" in {
      val initial = List(
        ("\"1\"^^xsd:int", true),
        ("\"1.1\"^^xsd:decimal", true),
        ("\"1.1\"^^xsd:float", true),
        ("\"1.1\"^^xsd:double", true),
        ("1", true),
        ("1.111111", true),
        ("-1.111", true),
        ("-1", true),
        ("0.0", true),
        ("0", true)
      ).toDF("input", "expected")

      val df = initial.withColumn("result", Func.isNumeric(initial("input")))

      df.collect.foreach { case Row(_, expected, result) =>
        expected shouldEqual result
      }
    }

    "return false when the term is not numeric" in {
      val initial = List(
        ("\"1200\"^^xsd:byte", false),
        ("\"1.1\"^^xsd:something", false),
        ("asdfsadfasdf", false),
        ("\"1.1\"", false)
      ).toDF("input", "expected")

      val df = initial.withColumn("result", Func.isNumeric(initial("input")))

      df.collect.foreach { case Row(_, expected, result) =>
        expected shouldEqual result
      }
    }
  }

  "Func.encodeForURI" should {

    "return correctly encoded URI" in {
      val initial = List(
        ("\"Los Angeles\"^^xsd:string", "Los%20Angeles"),
        ("\"Los Angeles\"@en", "Los%20Angeles"),
        ("Los Angeles", "Los%20Angeles"),
        ("~bébé", "~b%C3%A9b%C3%A9"),
        ("100% organic", "100%25%20organic"),
        (
          "http://www.example.com/00/Weather/CA/Los%20Angeles#ocean",
          "http%3A%2F%2Fwww.example.com%2F00%2FWeather%2FCA%2FLos%2520Angeles%23ocean"
        ),
        ("--", "--"),
        ("asdfsd345978a4534534fdsaf", "asdfsd345978a4534534fdsaf"),
        ("", "")
      ).toDF("input", "expected")

      val df = initial.withColumn("result", Func.encodeForURI(initial("input")))

      df.collect.foreach { case Row(_, expected, result) =>
        expected shouldEqual result
      }
    }
  }

  "Func.lang" should {

    "correctly return language tag" in {
      val initial = List(
        ("\"Los Angeles\"@en", "en"),
        ("\"Los Angeles\"@es", "es"),
        ("\"Los Angeles\"@en-US", "en-US"),
        ("Los Angeles", "")
      ).toDF("input", "expected")

      val df = initial.withColumn("result", Func.lang(initial("input")))

      df.collect.foreach { case Row(_, expected, result) =>
        expected shouldEqual result
      }
    }
  }

  "Func.langMatches" should {

    "correctly apply function when used with range" in {
      val initial = List(
        ("fr", true),
        ("fr-BE", true),
        ("en", false),
        ("", false)
      ).toDF("tags", "expected")

      val range = "FR"
      val df =
        initial.withColumn("result", Func.langMatches(initial("tags"), range))

      df.collect.foreach { case Row(_, expected, result) =>
        expected shouldEqual result
      }
    }

    "correctly apply function when used with wildcard" in {
      val initial = List(
        ("fr", true),
        ("fr-BE", true),
        ("en", true),
        ("", false)
      ).toDF("tags", "expected")

      val range = "*"
      val df =
        initial.withColumn("result", Func.langMatches(initial("tags"), range))

      df.collect.foreach { case Row(_, expected, result) =>
        expected shouldEqual result
      }
    }
  }

  def toRDFDateTime(datetime: TemporalAccessor): String =
    "\"" + DateTimeFormatter
      .ofPattern("yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]")
      .format(datetime) + "\"^^xsd:dateTime"
}
