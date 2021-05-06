package com.gsk.kg.sparqlparser

import com.gsk.kg.sparqlparser.BuiltInFunc._

import fastparse.MultiLineWhitespace._
import fastparse._

object BuiltInFuncParser {
  /*
  Functions on strings: https://www.w3.org/TR/sparql11-query/#func-strings
   */
  def uri[_: P]: P[Unit]       = P("uri")
  def concat[_: P]: P[Unit]    = P("concat")
  def str[_: P]: P[Unit]       = P("str")
  def strafter[_: P]: P[Unit]  = P("strafter")
  def strbefore[_: P]: P[Unit] = P("strbefore")
  def isBlank[_: P]: P[Unit]   = P("isBlank")
  def replace[_: P]: P[Unit]   = P("replace")
  def regex[_: P]: P[Unit]     = P("regex")
  def strends[_: P]: P[Unit]   = P("strends")
  def strstarts[_: P]: P[Unit] = P("strstarts")
  def strdt[_: P]: P[Unit]     = P("strdt")
  def substr[_: P]: P[Unit]    = P("substr")
  def strlen[_: P]: P[Unit]    = P("strlen")

  def uriParen[_: P]: P[URI] =
    P("(" ~ uri ~ ExpressionParser.parser ~ ")").map(s => URI(s))
  def concatParen[_: P]: P[CONCAT] =
    ("(" ~ concat ~ ExpressionParser.parser ~ ExpressionParser.parser ~ ")")
      .map { c =>
        CONCAT(c._1, c._2)
      }
  def strParen[_: P]: P[STR] =
    P("(" ~ str ~ ExpressionParser.parser ~ ")").map(s => STR(s))
  def strafterParen[_: P]: P[STRAFTER] = P(
    "(" ~ strafter ~ ExpressionParser.parser ~ ExpressionParser.parser ~ ")"
  ).map { s =>
    STRAFTER(s._1, s._2)
  }

  def strbeforeParen[_: P]: P[STRBEFORE] = P(
    "(" ~ strbefore ~ ExpressionParser.parser ~ ExpressionParser.parser ~ ")"
  ).map { s =>
    STRBEFORE(s._1, s._2)
  }

  def isBlankParen[_: P]: P[ISBLANK] =
    P("(" ~ isBlank ~ ExpressionParser.parser ~ ")").map(ISBLANK(_))

  def replaceParen[_: P]: P[REPLACE] = P(
    "(" ~ replace ~ ExpressionParser.parser ~ ExpressionParser.parser ~ ExpressionParser.parser ~ ")"
  ).map { s =>
    REPLACE(s._1, s._2, s._3)
  }

  def replaceWithFlagsParen[_: P]: P[REPLACE] = P(
    "(" ~ replace ~ ExpressionParser.parser ~ ExpressionParser.parser ~ ExpressionParser.parser ~ ExpressionParser.parser ~ ")"
  ).map { s =>
    REPLACE(s._1, s._2, s._3, s._4)
  }

  def regexParen[_: P]: P[REGEX] =
    P("(" ~ regex ~ ExpressionParser.parser ~ ExpressionParser.parser ~ ")")
      .map(f => REGEX(f._1, f._2))

  def regexWithFlagsParen[_: P]: P[REGEX] =
    P(
      "(" ~ regex ~ ExpressionParser.parser ~ ExpressionParser.parser ~ ExpressionParser.parser ~ ")"
    )
      .map(f => REGEX(f._1, f._2, f._3))

  def strendsParen[_: P]: P[STRENDS] =
    P("(" ~ strends ~ ExpressionParser.parser ~ ExpressionParser.parser ~ ")")
      .map(f => STRENDS(f._1, f._2))

  def strstartsParen[_: P]: P[STRSTARTS] =
    P("(" ~ strstarts ~ ExpressionParser.parser ~ ExpressionParser.parser ~ ")")
      .map(f => STRSTARTS(f._1, f._2))

  def strdtParen[_: P]: P[STRDT] =
    P("(" ~ strdt ~ ExpressionParser.parser ~ StringValParser.urival ~ ")")
      .map(f => STRDT(f._1, f._2))

  def substrParen[_: P]: P[SUBSTR] =
    P("(" ~ substr ~ ExpressionParser.parser ~ ExpressionParser.parser ~ ")")
      .map(f => SUBSTR(f._1, f._2))

  def substrWithLengthParen[_: P]: P[SUBSTR] =
    P(
      "(" ~ substr ~ ExpressionParser.parser ~ ExpressionParser.parser ~ ExpressionParser.parser ~ ")"
    )
      .map(f => SUBSTR(f._1, f._2, Option(f._3)))

  def strlenParen[_: P]: P[STRLEN] =
    P("(" ~ strlen ~ ExpressionParser.parser ~ ")")
      .map(f => STRLEN(f))

  def funcPatterns[_: P]: P[StringLike] =
    P(
      uriParen
        | concatParen
        | strParen
        | strafterParen
        | strbeforeParen
        | strendsParen
        | strstartsParen
        | substrParen
        | substrWithLengthParen
        | isBlankParen
        | replaceParen
        | replaceWithFlagsParen
        | regexParen
        | regexWithFlagsParen
        | strdtParen
        | strlenParen
    )
//      | StringValParser.string
//      | StringValParser.variable)

  def parser[_: P]: P[StringLike] = P(funcPatterns)
}
