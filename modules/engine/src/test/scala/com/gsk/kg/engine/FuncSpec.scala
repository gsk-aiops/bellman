package com.gsk.kg.engine

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.Row

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

  override implicit def reuseContextIfPossible: Boolean = true

  override implicit def enableHiveSupport: Boolean = false

  "Func.negate" should {

    "return the input boolean column negated" in {
      import sqlContext.implicits._

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
      import sqlContext.implicits._

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
      import sqlContext.implicits._

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
      import sqlContext.implicits._

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
      import sqlContext.implicits._

      val df = List("abracadabra").toDF("text")

      val result = df.select(Func.replace(df("text"), "bra", "*", "")).collect

      result shouldEqual Array(
        Row("a*cada*")
      )
    }

    "replace(abracadabra, a.*a, *) returns *" in {
      import sqlContext.implicits._

      val df = List("abracadabra").toDF("text")

      val result = df.select(Func.replace(df("text"), "a.*a", "*", "")).collect

      result shouldEqual Array(
        Row("*")
      )
    }

    "replace(abracadabra, a.*?a, *) returns *c*bra" in {
      import sqlContext.implicits._

      val df = List("abracadabra").toDF("text")

      val result = df.select(Func.replace(df("text"), "a.*?a", "*", "")).collect

      result shouldEqual Array(
        Row("*c*bra")
      )
    }

    "replace(abracadabra, a, \"\") returns brcdbr" in {
      import sqlContext.implicits._

      val df = List("abracadabra").toDF("text")

      val result = df.select(Func.replace(df("text"), "a", "", "")).collect

      result shouldEqual Array(
        Row("brcdbr")
      )
    }

    "replace(abracadabra, a(.), a$1$1) returns abbraccaddabbra" in {
      import sqlContext.implicits._

      val df = List("abracadabra").toDF("text")

      val result =
        df.select(Func.replace(df("text"), "a(.)", "a$1$1", "")).collect

      result shouldEqual Array(
        Row("abbraccaddabbra")
      )
    }

    "replace(abracadabra, .*?, $1) raises an error, because the pattern matches the zero-length string" in {
      import sqlContext.implicits._

      val df = List(
        "abracadabra"
      ).toDF("text")

      val caught = intercept[IndexOutOfBoundsException] {
        df.select(Func.replace(df("text"), ".*?", "$1", "")).collect
      }

      caught.getMessage shouldEqual "No group 1"
    }

    "replace(AAAA, A+, b) returns b" in {
      import sqlContext.implicits._

      val df = List("AAAA").toDF("text")

      val result = df.select(Func.replace(df("text"), "A+", "b", "")).collect

      result shouldEqual Array(
        Row("b")
      )
    }

    "replace(AAAA, A+?, b) returns bbbb" in {
      import sqlContext.implicits._

      val df = List(
        "AAAA"
      ).toDF("text")

      val result = df.select(Func.replace(df("text"), "A+?", "b", "")).collect

      result shouldEqual Array(
        Row("bbbb")
      )
    }

    "replace(darted, ^(.*?)d(.*)$, $1c$2) returns carted. (The first d is replaced.)" in {
      import sqlContext.implicits._

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
      import sqlContext.implicits._

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
      import sqlContext.implicits._

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
      import sqlContext.implicits._

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
  }

  "Func.strbefore" should {

    "find the correct string if it exists" in {
      import sqlContext.implicits._

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
      import sqlContext.implicits._

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
      import sqlContext.implicits._

      val df = List(
        "http://google.com",
        "http://other.com"
      ).toDF("text")

      df.select(Func.iri(df("text")).as("result")).collect shouldEqual Array(
        Row("http://google.com"),
        Row("http://other.com")
      )
    }
  }

  "Func.strends" should {

    "return true if a field ends with a given string" in {
      import sqlContext.implicits._

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
      import sqlContext.implicits._

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
      import sqlContext.implicits._

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
      import sqlContext.implicits._

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

  "Func.strdt" should {

    "return a literal with lexical for and type specified" in {
      import sqlContext.implicits._

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
      import sqlContext.implicits._

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
      import sqlContext.implicits._

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
      import sqlContext.implicits._

      val df = List(
        ("Hello", " Dolly"),
        ("Here's a song", " Dolly")
      ).toDF("a", "b")

      df.select(Func.concat(df("a"), df("b")).as("verses"))
        .collect shouldEqual Array(
        Row("Hello Dolly"),
        Row("Here's a song Dolly")
      )
    }

    "concatenate two string columns with quotes" in {
      import sqlContext.implicits._

      val df = List(
        ("\"Hello\"", "\" Dolly\""),
        ("\"Hello\"", " Dolly"),
        ("Hello", "\" Dolly\""),
        ("Hello", " Dolly")
      ).toDF("a", "b")

      df.select(Func.concat(df("a"), df("b")).as("verses"))
        .collect shouldEqual Array(
        Row("Hello Dolly"),
        Row("Hello Dolly"),
        Row("Hello Dolly"),
        Row("Hello Dolly")
      )
    }

    "concatenate a column in quotes with a literal string" in {
      import sqlContext.implicits._

      val df = List(
        ("Hello", " Dolly"),
        ("Here's a song", " Dolly")
      ).toDF("a", "b")

      df.select(Func.concat(df("a"), " world!").as("sentences"))
        .collect shouldEqual Array(
        Row("Hello world!"),
        Row("Here's a song world!")
      )
    }

    "concatenate a column with a literal string in quotes" in {
      import sqlContext.implicits._

      val df = List(
        ("\"Hello\"", " Dolly"),
        ("Here's a song", " Dolly")
      ).toDF("a", "b")

      df.select(Func.concat(df("a"), " world!").as("sentences"))
        .collect shouldEqual Array(
        Row("Hello world!"),
        Row("Here's a song world!")
      )
    }

    "concatenate a literal string with a column" in {
      import sqlContext.implicits._

      val df = List(
        ("Hello", " Dolly"),
        ("Here's a song", " Dolly")
      ).toDF("a", "b")

      df.select(Func.concat("Ciao", df("b")).as("verses"))
        .collect shouldEqual Array(
        Row("Ciao Dolly"),
        Row("Ciao Dolly")
      )
    }

    "concatenate a literal string with a column in quotes" in {
      import sqlContext.implicits._

      val df = List(
        ("Hello", "\" Dolly\""),
        ("Here's a song", " Dolly")
      ).toDF("a", "b")

      df.select(Func.concat("Ciao", df("b")).as("verses"))
        .collect shouldEqual Array(
        Row("Ciao Dolly"),
        Row("Ciao Dolly")
      )
    }
  }

  "Func.equals" should {
    "operate on equal dates correctly" in {
      import sqlContext.implicits._

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
      import sqlContext.implicits._

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

  "Func.parseDAteFromRDFDateTime" should {
    "work for all types of dates specified by RDF spec" in {
      import sqlContext.implicits._

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

  "Func.gt" should {
    "work for integer values" in {
      import sqlContext.implicits._

      val df = List(
        (2, 1)
      ).toDF("a", "b")

      df.select(Func.gt(df("a"), df("b"))).collect() shouldEqual Array(
        Row(true)
      )
    }

    "work in datetimes without a zone" in {
      import sqlContext.implicits._

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
      import sqlContext.implicits._

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
    "work for integer values" in {
      import sqlContext.implicits._

      val df = List(
        (1, 2)
      ).toDF("a", "b")

      df.select(Func.lt(df("a"), df("b"))).collect() shouldEqual Array(
        Row(true)
      )
    }

    "work in datetimes without a zone" in {
      import sqlContext.implicits._

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
      import sqlContext.implicits._

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
    "work for integer values" in {
      import sqlContext.implicits._

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
      import sqlContext.implicits._

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
      import sqlContext.implicits._

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
    "work for integer values" in {
      import sqlContext.implicits._

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
      import sqlContext.implicits._

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
      import sqlContext.implicits._

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
        import sqlContext.implicits._

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
        import sqlContext.implicits._

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

  "Func.sample" should {

    "return an arbitrary value from the column" in {
      import sqlContext.implicits._

      val elems = List(1, 2, 3, 4, 5)
      val df    = elems.toDF("a")

      elems.toSet should contain(
        df.select(Func.sample(df("a"))).collect().head.get(0)
      )
    }

  }

  def toRDFDateTime(datetime: TemporalAccessor): String =
    "\"" + DateTimeFormatter
      .ofPattern("yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]")
      .format(datetime) + "\"^^xsd:dateTime"
}
