package com.gsk.kg.engine

import com.gsk.kg.engine.scalacheck.CommonGenerators

import org.scalacheck.Gen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.gsk.kg.config.Config

class RdfFormatterSpec
    extends AnyFlatSpec
    with ScalaCheckDrivenPropertyChecks
    with Matchers
    with CommonGenerators
    with DataFrameSuiteBase {

  override implicit def reuseContextIfPossible: Boolean = true

  override implicit def enableHiveSupport: Boolean = false

  import RdfFormatter._

  "RDFNum" should "work for all possible instances of numbers" in {
    forAll(numberGen) { n =>
      RDFNum.unapply(n.toString()) shouldBe a[Some[_]]
    }
  }

  "RDFUri" should "match URIs correctly" in {
    forAll(uriGen) { uri =>
      RDFUri.unapply(uri.toString()) shouldBe a[Some[_]]
    }
  }

  "RDFBlank" should "match blank nodes correctly" in {
    forAll(blankGen) { blank =>
      RDFBlank.unapply(blank) shouldBe a[Some[_]]
    }
  }

  "RDFDataTypeLit" should "match literal with data type correctly" in {
    forAll(dataTypeLiteralGen) { dtl =>
      RDFDataTypeLiteral.unapply(dtl) shouldBe a[Some[_]]
    }
  }

  "RDFLocalizedString" should "match localized strings correctly" in {
    val localizedStringGen: Gen[String] =
      for {
        str  <- Gen.alphaStr
        lang <- Gen.oneOf("en", "es", "cn", "fr")
      } yield s""""$str"@$lang"""

    forAll(localizedStringGen) { str =>
      RDFLocalizedString.unapply(str) shouldBe a[Some[_]]
    }
  }

  "formatField" should "not raises exceptions when formatting" in {
    val gen = Gen.oneOf(
      uriGen.map(_.toString()),
      blankGen,
      numberGen,
      dataTypeLiteralGen
    )

    forAll(gen) { str =>
      noException shouldBe thrownBy {
        formatField(str)
      }
    }
  }

  val tests = List(
    ("test", "\"test\""),
    ("\"foo\"", "\"foo\""),
    ("\"\"bar\"\"", "\"bar\""),
    ("<http://test.com>", "<http://test.com>"),
    ("http://test.com", "http://test.com"),
    ("1", "1"),
    ("1.333", "1.333"),
    ("true", "true"),
    ("_:potato", "_:potato")
  )

  tests foreach { case (str, expected) =>
    it should s"format nodes correctly: $str -> $expected" in {
      formatField(str) shouldEqual expected
    }
  }

  "RdfFormatter" should "format all fields from a dataframe" in {
    import sqlContext.implicits._

    val df = List(
      ("string", "_:blanknode", "http://uri.com"),
      ("false", "true", "another string")
    ).toDF("s", "p", "o")

    val expected = List(
      ("\"string\"", "_:blanknode", "http://uri.com"),
      ("false", "true", "\"another string\"")
    ).toDF("s", "p", "o")

    val result = RdfFormatter.formatDataFrame(df, Config.default)

    expected.collect() shouldEqual result.collect()
  }

}
