package com.gsk.kg.sparqlparser

import com.gsk.kg.sparqlparser.PropertyExpression._

import fastparse.MultiLineWhitespace._
import fastparse._

object PropertyPathParser {

  def alt[_: P]: P[Unit]         = P("alt")
  def rev[_: P]: P[Unit]         = P("rev")
  def reverse[_: P]: P[Unit]     = P("reverse")
  def seq[_: P]: P[Unit]         = P("seq")
  def pathPlus[_: P]: P[Unit]    = P("path+")
  def pathStar[_: P]: P[Unit]    = P("path*")
  def pathOption[_: P]: P[Unit]  = P("path?")
  def notOneOf[_: P]: P[Unit]    = P("notoneof")
  def mod[_: P]: P[Unit]         = P("mod")
  def pathN[_: P]: P[Unit]       = P("pathN")
  def placeholder[_: P]: P[Unit] = P("_")

  def number[_: P]: P[Int] = {
    import NoWhitespace._
    P(CharIn("0-9").rep(1).!.map(_.toInt))
  }

  def uri[_: P]: P[Uri] =
    P("<" ~ CharsWhile(_ != '>') ~ ">").!.map(Uri)

  def alternativeParen[_: P]: P[Alternative] = P(
    "(" ~ alt ~ parser ~ parser ~ ")"
  ).map(p => Alternative(p._1, p._2))

  def revParen[_: P]: P[Reverse] = P(
    "(" ~ rev ~ parser ~ ")"
  ).map(Reverse)

  def reverseParen[_: P]: P[Reverse] = P(
    "(" ~ reverse ~ parser ~ ")"
  ).map(Reverse)

  def sequenceParen[_: P]: P[SeqExpression] = P(
    "(" ~ seq ~ parser ~ parser ~ ")"
  ).map(p => SeqExpression(p._1, p._2))

  def oneOrMoreParen[_: P]: P[OneOrMore] = P(
    "(" ~ pathPlus ~ parser ~ ")"
  ).map(OneOrMore)

  def zeroOrMoreParen[_: P]: P[ZeroOrMore] = P(
    "(" ~ pathStar ~ parser ~ ")"
  ).map(ZeroOrMore)

  def zeroOrOneParen[_: P]: P[ZeroOrOne] = P(
    "(" ~ pathOption ~ parser ~ ")"
  ).map(ZeroOrOne)

  def notOneOfParen[_: P]: P[NotOneOf] = P(
    "(" ~ notOneOf ~ parser.rep(1) ~ ")"
  ).map(p => NotOneOf(p.toList))

  def betweenNAndMParen[_: P]: P[BetweenNAndM] = P(
    "(" ~ mod ~ number ~ number ~ parser ~ ")"
  ).map(p => BetweenNAndM(p._1, p._2, p._3))

  def exactlyNParen[_: P]: P[ExactlyN] = P(
    "(" ~ pathN ~ number ~ parser ~ ")"
  ).map(p => ExactlyN(p._1, p._2))

  def nOrMoreParen[_: P]: P[NOrMore] = P(
    "(" ~ mod ~ number ~ placeholder ~ parser ~ ")"
  ).map(p => NOrMore(p._1, p._2))

  def betweenNZeroAndN[_: P]: P[BetweenZeroAndN] = P(
    "(" ~ mod ~ placeholder ~ number ~ parser ~ ")"
  ).map(p => BetweenZeroAndN(p._1, p._2))

  def parser[_: P]: P[PropertyExpression] = P(
    uri
      | alternativeParen
      | reverseParen
      | revParen
      | sequenceParen
      | oneOrMoreParen
      | zeroOrMoreParen
      | zeroOrOneParen
      | notOneOfParen
      | betweenNAndMParen
      | exactlyNParen
      | nOrMoreParen
      | betweenNZeroAndN
  )
}
