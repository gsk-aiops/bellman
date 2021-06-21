package com.gsk.kg.sparqlparser

import com.gsk.kg.sparqlparser.Arithmetic._

import fastparse.MultiLineWhitespace._
import fastparse._

object ArithmeticParser {

  def add[_: P]: P[Unit]      = P("+")
  def subtract[_: P]: P[Unit] = P("-")
  def multiply[_: P]: P[Unit] = P("*")
  def divide[_: P]: P[Unit]   = P("/")

  def addParen[_: P]: P[ADD] =
    P("(" ~ add ~ ExpressionParser.parser ~ ExpressionParser.parser ~ ")")
      .map { case (l, r) => ADD(l, r) }

  def subtractParen[_: P]: P[SUBTRACT] =
    P("(" ~ subtract ~ ExpressionParser.parser ~ ExpressionParser.parser ~ ")")
      .map { case (l, r) => SUBTRACT(l, r) }

  def multiplyParen[_: P]: P[MULTIPLY] =
    P("(" ~ multiply ~ ExpressionParser.parser ~ ExpressionParser.parser ~ ")")
      .map { case (l, r) => MULTIPLY(l, r) }

  def divideParen[_: P]: P[DIVIDE] =
    P("(" ~ divide ~ ExpressionParser.parser ~ ExpressionParser.parser ~ ")")
      .map { case (l, r) => DIVIDE(l, r) }

  def parser[_: P]: P[Arithmetic] =
    P(
      addParen
        | subtractParen
        | multiplyParen
        | divideParen
    )
}
