package com.gsk.kg.sparqlparser

import com.gsk.kg.sparqlparser.MathFunc._

import fastparse.MultiLineWhitespace._
import fastparse._

object MathFuncParser {

  def ceil[_: P]: P[Unit]  = P("ceil")
  def round[_: P]: P[Unit] = P("round")
  def rand[_: P]: P[Unit]  = P("rand")
  def abs[_: P]: P[Unit]   = P("abs")
  def floor[_: P]: P[Unit] = P("floor")

  def ceilParen[_: P]: P[CEIL] =
    P("(" ~ ceil ~ ExpressionParser.parser ~ ")")
      .map(f => CEIL(f))

  def roundParen[_: P]: P[ROUND] =
    P("(" ~ round ~ ExpressionParser.parser ~ ")")
      .map(f => ROUND(f))

  def randParen[_: P]: P[RAND] =
    P("(" ~ rand ~ ")")
      .map(f => RAND())

  def absParen[_: P]: P[ABS] =
    P("(" ~ abs ~ ExpressionParser.parser ~ ")")
      .map(f => ABS(f))

  def floorParen[_: P]: P[FLOOR] =
    P("(" ~ floor ~ ExpressionParser.parser ~ ")")
      .map(f => FLOOR(f))

  def funcPatterns[_: P]: P[StringLike] =
    P(
      ceilParen
        | roundParen
        | randParen
        | absParen
        | floorParen
    )

  def parser[_: P]: P[StringLike] = P(funcPatterns)
}
