package com.gsk.kg.sparqlparser

import higherkindness.droste.macros.deriveFixedPoint

import cats.implicits._

@deriveFixedPoint trait PropertyExpression

object PropertyExpression {
  final case class Alternative(pel: PropertyExpression, per: PropertyExpression)
      extends PropertyExpression
  final case class Reverse(e: PropertyExpression) extends PropertyExpression
  final case class SeqExpression(
      pel: PropertyExpression,
      per: PropertyExpression
  )                                                  extends PropertyExpression
  final case class OneOrMore(e: PropertyExpression)  extends PropertyExpression
  final case class ZeroOrMore(e: PropertyExpression) extends PropertyExpression
  final case class ZeroOrOne(e: PropertyExpression)  extends PropertyExpression
  final case class NotOneOf(es: List[PropertyExpression])
      extends PropertyExpression
  final case class BetweenNAndM(n: Int, m: Int, e: PropertyExpression)
      extends PropertyExpression
  final case class ExactlyN(n: Int, e: PropertyExpression)
      extends PropertyExpression
  final case class NOrMore(n: Int, e: PropertyExpression)
      extends PropertyExpression
  final case class BetweenZeroAndN(n: Int, e: PropertyExpression)
      extends PropertyExpression
  final case class Uri(s: String) extends PropertyExpression
}
