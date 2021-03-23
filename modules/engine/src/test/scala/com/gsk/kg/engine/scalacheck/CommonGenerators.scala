package com.gsk.kg.engine
package scalacheck

import java.net.URI

import org.scalacheck.Arbitrary
import org.scalacheck.Gen

trait CommonGenerators {

  val numberGen: Gen[Number] = Arbitrary.arbNumber.arbitrary

  val blankGen: Gen[String] = Gen.alphaNumStr.map(s => "_:" + s)

  val uriGen: Gen[URI] =
    for {
      scheme   <- Gen.oneOf("http", "https", "ftp")
      host     <- Gen.oneOf("gsk.com", "gsk-id", "dbpedia") //String host,
      port     <- Gen.option(Gen.choose(1025, 65535))
      path     <- path
      query    <- query
      fragment <- Gen.option(Gen.alphaNumStr)
    } yield new URI(
      scheme + "://" +
        host +
        port.map(p => s":$p").getOrElse("") + "/" +
        path + "?" +
        query +
        fragment.map(f => s"#$f").getOrElse("")
    )

  val nonEmptyStringGenerator =
    Gen.nonEmptyListOf(Gen.alphaChar).map(_.mkString)

  val path: Gen[String] =
    for {
      n     <- Gen.choose(0, 10)
      paths <- Gen.listOfN(n, nonEmptyStringGenerator)
    } yield paths.mkString("/")

  val query: Gen[String] =
    for {
      n <- Gen.choose(0, 10)
      paths <- Gen.listOfN(
        n,
        Gen.zip(nonEmptyStringGenerator, nonEmptyStringGenerator)
      )
    } yield paths.map({ case (k, v) => s"$k=$v" }).mkString("&")

  val sparqlDataTypesGen: Gen[String] = Gen.oneOf(
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

  val dataTypeLiteralGen: Gen[String] =
    for {
      dataType <- sparqlDataTypesGen
      lit      <- Gen.alphaNumStr
    } yield s""""${lit}"^^$dataType"""

  def smallListOf[A](a: Gen[A]): Gen[List[A]] =
    Gen.choose(0, 5).flatMap(n => Gen.listOfN(n, a))

  def smallNonEmptyListOf[A](a: Gen[A]): Gen[List[A]] =
    Gen.choose(1, 5).flatMap(n => Gen.listOfN(n, a))

}

object CommonGenerators extends CommonGenerators
