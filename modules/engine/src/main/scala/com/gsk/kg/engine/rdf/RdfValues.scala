package com.gsk.kg.engine.rdf

import com.gsk.kg.engine.rdf.RdfValue._

import frameless.Injection

sealed trait RdfValue {
  override def toString(): String =
    this match {
      case RdfUri(value)                => s"<$value>"
      case RdfString(value, None)       => s""""$value""""
      case RdfString(value, Some(lang)) => s"""""$value"@$lang"""
      case RdfLiteral(value, datatype)  => s""""$value"^^<$datatype>"""
      case RdfInt(value) =>
        s""""$value"^^<http://www.w3.org/2001/XMLSchema#int>"""
      case RdfBoolean(value) =>
        s""""$value"^^<http://www.w3.org/2001/XMLSchema#boolean>"""
      case RdfDouble(value) =>
        s""""$value"^^<http://www.w3.org/2001/XMLSchema#double>"""
      case RdfDecimal(value) =>
        s""""$value"^^<http://www.w3.org/2001/XMLSchema#decimal>"""
      case RdfBlank(value) =>
        s"""_:$value"""
    }
}
object RdfValue {

  final case class RdfString(value: String, tag: Option[String])
      extends RdfValue
  final case class RdfUri(value: String)                       extends RdfValue
  final case class RdfLiteral(value: String, datatype: String) extends RdfValue
  final case class RdfBoolean(value: Boolean)                  extends RdfValue
  final case class RdfInt(value: Int)                          extends RdfValue
  final case class RdfDouble(value: Double)                    extends RdfValue
  final case class RdfDecimal(value: BigDecimal)               extends RdfValue
  final case class RdfBlank(value: String)                     extends RdfValue

  implicit val injection: Injection[RdfValue, String] =
    new Injection[RdfValue, String] {
      def invert(a: String): RdfValue =
        fastparse.parse(a, parser.parse(_)).get.value
      def apply(b: RdfValue): String =
        b.toString
    }
}

final case class RdfTriple(s: RdfValue, p: RdfValue, o: RdfValue)
