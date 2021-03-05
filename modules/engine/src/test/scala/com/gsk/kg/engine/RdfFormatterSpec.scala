package com.gsk.kg.engine

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.scalacheck.Gen
import org.scalacheck.Arbitrary
import java.net.URI

class RdfFormatterSpec
    extends AnyFlatSpec
    with ScalaCheckDrivenPropertyChecks
    with Matchers {

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

  "formatField" should "not raises exceptions when formatting" in {
    val gen = Gen.oneOf(
      uriGen.map(_.toString()),
      blankGen,
      numberGen.map(_.toString()),
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
    ("<http://test.com>", "<http://test.com>"),
    ("http://test.com", "<http://test.com>"),
    ("1", "1"),
    ("1.333", "1.333"),
    ("_:potato", "_:potato")
  )

  tests foreach {
    case (str, expected) =>
      it should s"format nodes correctly: $str -> $expected" in {
        formatField(str) shouldEqual expected
      }
  }

  private val numberGen: Gen[Number] = Arbitrary.arbNumber.arbitrary

  private val blankGen: Gen[String] = Gen.alphaNumStr.map(s => "_:" + s)

  private val uriGen: Gen[URI] =
    for {
      scheme <- Gen.oneOf("http", "https", "ftp")
      host <- Gen.oneOf("gsk.com", "gsk-id", "dbpedia") //String host,
      port <- Gen.option(Gen.choose(1025, 65535))
      path <- path
      query <- query
      fragment <- Gen.option(Gen.alphaNumStr)
    } yield new URI(
      scheme + "://" +
        host +
        port.map(p => s":$p").getOrElse("") + "/" +
        path + "?" +
        query +
        fragment.map(f => s"#$f").getOrElse("")
    )

  private val sparqlDataTypesGen: Gen[String] = Gen.oneOf(
    "xsd:string",
    "xsd:integer",
    "xsd:int",
    "xsd:float",
    "xsd:decimal",
    "xsd:double",
    "xsd:long",
    "xsd:boolean",
    "xsd:date",
    "xsd:dateTime",
    "xsd:negativeInteger",
    "xsd:nonNegativeInteger",
    "xsd:positiveInteger",
    "xsd:nonPositiveInteger",
    "xsd:short",
    "xsd:byte",
    "xsd:time",
    "xsd:unsignedByte",
    "xsd:unsignedLong",
    "xsd:unsignedShort"
  )

  private val dataTypeLiteralGen: Gen[String] =
    for {
      dataType <- sparqlDataTypesGen
      lit <- Gen.alphaNumStr
    } yield s""""${lit}"^^$dataType"""

  private val path: Gen[String] =
    for {
      n <- Gen.choose(0, 10)
      paths <- Gen.listOfN(n, Gen.alphaStr)
    } yield paths.mkString("/")

  private val query: Gen[String] =
    for {
      n <- Gen.choose(0, 10)
      paths <- Gen.listOfN(n, Gen.zip(Gen.alphaStr, Gen.alphaStr))
    } yield paths.map({ case (k, v) => s"$k=$v" }).mkString("&")

}
