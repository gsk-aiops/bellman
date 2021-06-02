package com.gsk.kg.engine.functions

import cats.syntax.list._

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.lit

import com.gsk.kg.engine.compiler.SparkSpec
import com.gsk.kg.engine.scalacheck.CommonGenerators

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class FuncStringsSpec
    extends AnyWordSpec
    with Matchers
    with SparkSpec
    with ScalaCheckDrivenPropertyChecks
    with CommonGenerators {

  import sqlContext.implicits._

  override implicit def reuseContextIfPossible: Boolean = true

  override implicit def enableHiveSupport: Boolean = false

  "Funcs on strings" when {

    "FuncStrings.strlen" should {

      "count characters on plain string" in {
        val df = List(
          "chat"
        ).toDF("a")

        df.select(FuncStrings.strlen(df("a"))).collect shouldEqual Array(
          Row(4)
        )
      }

      "count characters on typed string" in {
        val df = List(
          "\"chat\"^^xsd:string"
        ).toDF("a")

        df.select(FuncStrings.strlen(df("a"))).collect shouldEqual Array(
          Row(4)
        )
      }

      "count characters on localized string" in {
        val df = List(
          "\"chat\"@en"
        ).toDF("a")

        df.select(FuncStrings.strlen(df("a"))).collect shouldEqual Array(
          Row(4)
        )
      }
    }

    "FuncStrings.substr" should {

      "correctly return the substring of a given column without length specified" in {

        val df = List(
          "hello world",
          "hello universe"
        ).toDF("text")

        df.select(FuncStrings.substr(df("text"), 5, None).as("result"))
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

        df.select(FuncStrings.substr(df("text"), 5, Some(3)).as("result"))
          .collect shouldEqual Array(
          Row("o w"),
          Row("o u")
        )
      }
    }

    "FuncStrings.ucase" should {

      "convert all lexical characters to upper case" in {

        val df = List(
          "hello",
          "\"hello\"@en",
          "\"hello\"^^xsd:string"
        ).toDF("text")

        df.select(FuncStrings.ucase(df("text")).as("result"))
          .collect shouldEqual Array(
          Row("HELLO"),
          Row("\"HELLO\"@en"),
          Row("\"HELLO\"^^xsd:string")
        )
      }
    }

    "FuncStrings.lcase" should {

      "convert all lexical characters to lower case" in {

        val df = List(
          "HELLO",
          "\"HELLO\"@en",
          "\"HELLO\"^^xsd:string"
        ).toDF("text")

        df.select(FuncStrings.lcase(df("text")).as("result"))
          .collect shouldEqual Array(
          Row("hello"),
          Row("\"hello\"@en"),
          Row("\"hello\"^^xsd:string")
        )
      }
    }

    "FuncStrings.strstarts" should {

      "return true if a field starts with a given string" in {

        val df = List(
          "hello world",
          "hello universe"
        ).toDF("text")

        df.select(FuncStrings.strstarts(df("text"), "hello").as("result"))
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

        df.select(FuncStrings.strstarts(df("text"), "help").as("result"))
          .collect shouldEqual Array(
          Row(false),
          Row(false)
        )
      }
    }

    "FuncStrings.strends" should {

      "return true if a field ends with a given string" in {

        val df = List(
          "sports car",
          "sedan car"
        ).toDF("text")

        df.select(FuncStrings.strends(df("text"), "car").as("result"))
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

        df.select(FuncStrings.strends(df("text"), "dses").as("result"))
          .collect shouldEqual Array(
          Row(false),
          Row(false)
        )
      }
    }

    "FuncStrings.strbefore" should {

      "find the correct string if it exists" in {

        val df = List(
          "hello potato",
          "goodbye tomato"
        ).toDF("text")

        df.select(FuncStrings.strbefore(df("text"), " ").as("result"))
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

        df.select(FuncStrings.strbefore(df("text"), "#").as("result"))
          .collect shouldEqual Array(
          Row(""),
          Row("")
        )
      }
    }

    "FuncStrings.strafter" should {

      "find the correct string if it exists" in {

        val df = List(
          "hello#potato",
          "goodbye#tomato"
        ).toDF("text")

        df.select(FuncStrings.strafter(df("text"), "#").as("result"))
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

        df.select(FuncStrings.strafter(df("text"), "#").as("result"))
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
          val strafter = FuncStrings.strafter(df("arg1"), arg2)
          val result = df
            .select(strafter)
            .as("result")
            .collect()

          result shouldEqual Array(Row(expect))
        }
      }
    }

    "FuncStrings.encodeForURI" should {

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

        val df = initial.withColumn(
          "result",
          FuncStrings.encodeForURI(initial("input"))
        )

        df.collect.foreach { case Row(_, expected, result) =>
          expected shouldEqual result
        }
      }
    }

    "FuncStrings.concat" should {

      "concatenate two string columns" in {

        val df = List(
          ("Hello", " Dolly"),
          ("Here's a song", " Dolly")
        ).toDF("a", "b")

        df.select(
          FuncStrings.concat(df("a"), List(df("b")).toNel.get).as("verses")
        ).collect shouldEqual Array(
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

        df.select(
          FuncStrings.concat(df("a"), List(df("b")).toNel.get).as("verses")
        ).collect shouldEqual Array(
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
          FuncStrings
            .concat(df("a"), List(lit(" world!")).toNel.get)
            .as("sentences")
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
          FuncStrings
            .concat(df("a"), List(lit(" world!")).toNel.get)
            .as("sentences")
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

        df.select(
          FuncStrings.concat(lit("Ciao"), List(df("b")).toNel.get).as("verses")
        ).collect shouldEqual Array(
          Row("Ciao Dolly"),
          Row("Ciao Dolly")
        )
      }

      "concatenate a literal string with a column in quotes" in {

        val df = List(
          ("Hello", "\" Dolly\""),
          ("Here's a song", " Dolly")
        ).toDF("a", "b")

        df.select(
          FuncStrings.concat(lit("Ciao"), List(df("b")).toNel.get).as("verses")
        ).collect shouldEqual Array(
          Row("Ciao Dolly"),
          Row("Ciao Dolly")
        )
      }

      "concatenate mixing literals and string columns multiple times" in {

        val df = List(
          ("Hello", "\" Dolly\""),
          ("Here's a song", " Dolly")
        ).toDF("a", "b")

        df.select(
          FuncStrings.concat(lit("Ciao"), List(df("b")).toNel.get).as("verses")
        ).collect shouldEqual Array(
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
          val concat = FuncStrings.concat(df("arg1"), List(lit(arg2)).toNel.get)
          val result =
            df.select(concat).as("result").collect()
          result shouldEqual Array(Row(expected))
        }
      }
    }

    "FuncStrings.langMatches" should {

      "correctly apply function when used with range" in {
        val initial = List(
          ("fr", true),
          ("fr-BE", true),
          ("en", false),
          ("", false)
        ).toDF("tags", "expected")

        val range = "FR"
        val df =
          initial.withColumn(
            "result",
            FuncStrings.langMatches(initial("tags"), range)
          )

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
          initial.withColumn(
            "result",
            FuncStrings.langMatches(initial("tags"), range)
          )

        df.collect.foreach { case Row(_, expected, result) =>
          expected shouldEqual result
        }
      }
    }

    "FuncStrings.regex" should {

      "return true if a field matches the given regex pattern" in {

        val df = List(
          "Alice",
          "Alison"
        ).toDF("text")

        df.select(FuncStrings.regex(df("text"), "^ali", "i").as("result"))
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

        df.select(FuncStrings.regex(df("text"), "^ali", "").as("result"))
          .collect shouldEqual Array(
          Row(false),
          Row(false)
        )
      }
    }

    "FuncStrings.replace" should {

      "replace when pattern occurs" in {

        val df = List(
          "abcd",
          "abaB",
          "bbBB",
          "aaaa"
        ).toDF("text")

        val result =
          df.select(FuncStrings.replace(df("text"), "b", "Z", "")).collect

        result shouldEqual Array(
          Row("aZcd"),
          Row("aZaB"),
          Row("ZZBB"),
          Row("aaaa")
        )
      }

      "replace(abracadabra, bra, *) returns a*cada*" in {

        val df = List("abracadabra").toDF("text")

        val result =
          df.select(FuncStrings.replace(df("text"), "bra", "*", "")).collect

        result shouldEqual Array(
          Row("a*cada*")
        )
      }

      "replace(abracadabra, a.*a, *) returns *" in {

        val df = List("abracadabra").toDF("text")

        val result =
          df.select(FuncStrings.replace(df("text"), "a.*a", "*", "")).collect

        result shouldEqual Array(
          Row("*")
        )
      }

      "replace(abracadabra, a.*?a, *) returns *c*bra" in {

        val df = List("abracadabra").toDF("text")

        val result =
          df.select(FuncStrings.replace(df("text"), "a.*?a", "*", "")).collect

        result shouldEqual Array(
          Row("*c*bra")
        )
      }

      "replace(abracadabra, a, \"\") returns brcdbr" in {

        val df = List("abracadabra").toDF("text")

        val result =
          df.select(FuncStrings.replace(df("text"), "a", "", "")).collect

        result shouldEqual Array(
          Row("brcdbr")
        )
      }

      "replace(abracadabra, a(.), a$1$1) returns abbraccaddabbra" in {

        val df = List("abracadabra").toDF("text")

        val result =
          df.select(FuncStrings.replace(df("text"), "a(.)", "a$1$1", ""))
            .collect

        result shouldEqual Array(
          Row("abbraccaddabbra")
        )
      }

      "replace(abracadabra, .*?, $1) raises an error, because the pattern matches the zero-length string" in {

        val df = List(
          "abracadabra"
        ).toDF("text")

        val caught = intercept[IndexOutOfBoundsException] {
          df.select(FuncStrings.replace(df("text"), ".*?", "$1", "")).collect
        }

        caught.getMessage shouldEqual "No group 1"
      }

      "replace(AAAA, A+, b) returns b" in {

        val df = List("AAAA").toDF("text")

        val result =
          df.select(FuncStrings.replace(df("text"), "A+", "b", "")).collect

        result shouldEqual Array(
          Row("b")
        )
      }

      "replace(AAAA, A+?, b) returns bbbb" in {

        val df = List(
          "AAAA"
        ).toDF("text")

        val result =
          df.select(FuncStrings.replace(df("text"), "A+?", "b", "")).collect

        result shouldEqual Array(
          Row("bbbb")
        )
      }

      "replace(darted, ^(.*?)d(.*)$, $1c$2) returns carted. (The first d is replaced.)" in {

        val df = List(
          "darted"
        ).toDF("text")

        val result =
          df.select(
            FuncStrings.replace(df("text"), "^(.*?)d(.*)$", "$1c$2", "")
          ).collect

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

        val result =
          df.select(FuncStrings.replace(df("text"), "b", "Z", "i")).collect

        result shouldEqual Array(
          Row("aZcd"),
          Row("aZaZ"),
          Row("ZZZZ"),
          Row("aaaa")
        )
      }
    }
  }
}
