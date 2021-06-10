package com.gsk.kg.sparqlparser

import com.gsk.kg.sparqlparser.Conditional._

import fastparse.MultiLineWhitespace._
import fastparse._

object ConditionalParser {
  def equals[_: P]: P[Unit]    = P("=")
  def notEquals[_: P]: P[Unit] = P("!=")
  def gt[_: P]: P[Unit]        = P(">")
  def gte[_: P]: P[Unit]       = P(">=")
  def lt[_: P]: P[Unit]        = P("<")
  def lte[_: P]: P[Unit]       = P("<=")
  def and[_: P]: P[Unit]       = P("&&")
  def or[_: P]: P[Unit]        = P("||")
  def negate[_: P]: P[Unit]    = P("!")
  def in[_: P]: P[Unit]        = P("in")
  def notIn[_: P]: P[Unit]     = P("notin")
  def sameTerm[_: P]: P[Unit]  = P("sameTerm")
  def `if`[_: P]: P[Unit]      = P("if")
  def bound[_: P]: P[Unit]     = P("bound")

  def equalsParen[_: P]: P[EQUALS] =
    P("(" ~ equals ~ ExpressionParser.parser ~ ExpressionParser.parser ~ ")")
      .map(f => EQUALS(f._1, f._2))

  def notEqualsParen[_: P]: P[NEGATE] =
    P("(" ~ notEquals ~ ExpressionParser.parser ~ ExpressionParser.parser ~ ")")
      .map(f => NEGATE(EQUALS(f._1, f._2)))

  def gtParen[_: P]: P[GT] =
    P("(" ~ gt ~ ExpressionParser.parser ~ ExpressionParser.parser ~ ")").map(
      f => GT(f._1, f._2)
    )

  def gteParen[_: P]: P[GTE] =
    P("(" ~ gte ~ ExpressionParser.parser ~ ExpressionParser.parser ~ ")").map(
      f => GTE(f._1, f._2)
    )

  def ltParen[_: P]: P[LT] =
    P("(" ~ lt ~ ExpressionParser.parser ~ ExpressionParser.parser ~ ")").map(
      f => LT(f._1, f._2)
    )

  def lteParen[_: P]: P[LTE] =
    P("(" ~ lte ~ ExpressionParser.parser ~ ExpressionParser.parser ~ ")").map(
      f => LTE(f._1, f._2)
    )

  def andParen[_: P]: P[AND] =
    P("(" ~ and ~ ExpressionParser.parser ~ ExpressionParser.parser ~ ")").map(
      f => AND(f._1, f._2)
    )
  def orParen[_: P]: P[OR] =
    P("(" ~ or ~ ExpressionParser.parser ~ ExpressionParser.parser ~ ")").map(
      f => OR(f._1, f._2)
    )

  def negateParen[_: P]: P[NEGATE] =
    P("(" ~ negate ~ ExpressionParser.parser ~ ")")
      .map(NEGATE)

  def inParen[_: P]: P[IN] =
    P("(" ~ in ~ ExpressionParser.parser ~ ExpressionParser.parser.rep(0) ~ ")")
      .map(f => IN(f._1, f._2.toList))

  def notInParen[_: P]: P[Conditional] =
    P(
      "(" ~ notIn ~ ExpressionParser.parser ~ ExpressionParser.parser.rep(
        0
      ) ~ ")"
    )
      .map(f => NEGATE(IN(f._1, f._2.toList)))

  def sameTermParen[_: P]: P[SAMETERM] =
    P("(" ~ sameTerm ~ ExpressionParser.parser ~ ExpressionParser.parser ~ ")")
      .map(f => SAMETERM(f._1, f._2))

  def ifParen[_: P]: P[IF] =
    P(
      "(" ~ `if` ~ ExpressionParser.parser ~ ExpressionParser.parser ~ ExpressionParser.parser ~ ")"
    )
      .map(f => IF(f._1, f._2, f._3))

  def boundParen[_: P]: P[BOUND] =
    P(
      "(" ~ bound ~ StringValParser.variable ~ ")"
    ).map(BOUND)

  def parser[_: P]: P[Conditional] =
    P(
      notEqualsParen
        | equalsParen
        | gteParen
        | lteParen
        | gtParen
        | ltParen
        | andParen
        | orParen
        | negateParen
        | inParen
        | notInParen
        | sameTermParen
        | ifParen
        | boundParen
    )
}
