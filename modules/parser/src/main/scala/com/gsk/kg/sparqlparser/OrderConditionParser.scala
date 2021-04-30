package com.gsk.kg.sparqlparser

import com.gsk.kg.sparqlparser.ConditionOrder.ASC
import com.gsk.kg.sparqlparser.ConditionOrder.DESC
import fastparse.MultiLineWhitespace._
import fastparse._

object OrderConditionParser {

  def asc[_: P]: P[Unit]  = P("asc")
  def desc[_: P]: P[Unit] = P("desc")

  def conditionParen[_: P]: P[Expression] = P(
    BuiltInFuncParser.parser | ConditionalParser.parser
  )

  def ascParen[_: P]: P[ASC] = P(
    "(" ~ asc ~ (StringValParser.variable | conditionParen) ~ ")" |
      StringValParser.variable |
      conditionParen
  ).map(p => ASC(p))

  def descParen[_: P]: P[DESC] = P(
    "(" ~ desc ~ (StringValParser.variable | conditionParen) ~ ")"
  ).map(p => DESC(p))

  def parser[_: P]: P[ConditionOrder] = P(
    ascParen |
      descParen
  )
}
