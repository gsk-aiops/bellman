package com.gsk.kg.engine.rdf

import com.gsk.kg.sparqlparser.StringValParser._

import fastparse.MultiLineWhitespace._
import fastparse._

object parser {

  def uri[_: P]: P[RdfValue.RdfUri] = iri.map(RdfValue.RdfUri.apply)

  def literal[_: P]: P[RdfValue.RdfLiteral] =
    (emptyDataTypeString | dataTypeString).map { dt =>
      RdfValue.RdfLiteral(dt.s, dt.tag)
    }

  def int[A](implicit P: P[A]): P[RdfValue.RdfInt] =
    (emptyDataTypeString | dataTypeString).flatMap { dt =>
      val ints = Set(
        "http://www.w3.org/2001/XMLSchema#int",
        "http://www.w3.org/2001/XMLSchema#integer",
        "xsd:int",
        "xsd:integer"
      )
      if (ints.contains(dt.tag)) {
        P.freshSuccess(RdfValue.RdfInt(dt.s.toInt))
      } else {
        P.freshFailure()
      }
    }

  def double[A](implicit P: P[A]): P[RdfValue.RdfDouble] =
    (emptyDataTypeString | dataTypeString).flatMap { dt =>
      val doubles = Set(
        "http://www.w3.org/2001/XMLSchema#double",
        "xsd:double"
      )
      if (doubles.contains(dt.tag)) {
        P.freshSuccess(RdfValue.RdfDouble(dt.s.toDouble))
      } else {
        P.freshFailure()
      }
    }

  def decimal[A](implicit P: P[A]): P[RdfValue.RdfDecimal] =
    (emptyDataTypeString | dataTypeString).flatMap { dt =>
      val decimals = Set(
        "http://www.w3.org/2001/XMLSchema#decimal",
        "xsd:decimal"
      )
      if (decimals.contains(dt.tag)) {
        P.freshSuccess(RdfValue.RdfDecimal(dt.s.toInt))
      } else {
        P.freshFailure()
      }
    }

  def boolean[A](implicit P: P[A]): P[RdfValue.RdfBoolean] =
    (emptyDataTypeString | dataTypeString).flatMap { dt =>
      val booleans = Set(
        "http://www.w3.org/2001/XMLSchema#boolean",
        "xsd:boolean"
      )
      if (booleans.contains(dt.tag)) {
        P.freshSuccess(RdfValue.RdfBoolean(dt.s.toBoolean))
      } else {
        P.freshFailure()
      }
    }

  def stringFromLiteral[A](implicit P: P[A]): P[RdfValue.RdfString] =
    (emptyDataTypeString | dataTypeString).flatMap { dt =>
      val strings = Set(
        "http://www.w3.org/2001/XMLSchema#string",
        "xsd:string"
      )
      if (strings.contains(dt.tag)) {
        P.freshSuccess(RdfValue.RdfString(dt.s, None))
      } else {
        P.freshFailure()
      }
    }

  def stringFromLangString[_: P]: P[RdfValue.RdfString] =
    (emptyLangString | langString).map(ls =>
      RdfValue.RdfString(ls.s, Some(ls.tag))
    )

  def stringFromPlainString[_: P]: P[RdfValue.RdfString] =
    (emptyString | plainString).map(ls => RdfValue.RdfString(ls.s, None))

  def rdfString[_: P]: P[RdfValue.RdfString] =
    stringFromLangString | stringFromLiteral | stringFromPlainString

  def blank[_: P]: P[RdfValue.RdfBlank] =
    blankNode.map(bn => RdfValue.RdfBlank(bn.s))

  def parse[_: P]: P[RdfValue] = P(
    blank | uri | rdfString | boolean | int | double | decimal | literal
  )

}
