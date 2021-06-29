package com.gsk.kg.sparqlparser

import com.gsk.kg.sparqlparser.BuiltInFunc._

import fastparse.MultiLineWhitespace._
import fastparse._

object BuiltInFuncParser {
  /*
  Functions on strings: https://www.w3.org/TR/sparql11-query/#func-strings
   */
  def uri[_: P]: P[Unit]          = P("uri")
  def concat[_: P]: P[Unit]       = P("concat")
  def str[_: P]: P[Unit]          = P("str")
  def strafter[_: P]: P[Unit]     = P("strafter")
  def strbefore[_: P]: P[Unit]    = P("strbefore")
  def isBlank[_: P]: P[Unit]      = P("isBlank")
  def replace[_: P]: P[Unit]      = P("replace")
  def regex[_: P]: P[Unit]        = P("regex")
  def strends[_: P]: P[Unit]      = P("strends")
  def strstarts[_: P]: P[Unit]    = P("strstarts")
  def strdt[_: P]: P[Unit]        = P("strdt")
  def substr[_: P]: P[Unit]       = P("substr")
  def strlen[_: P]: P[Unit]       = P("strlen")
  def lcase[_: P]: P[Unit]        = P("lcase")
  def ucase[_: P]: P[Unit]        = P("ucase")
  def isLiteral[_: P]: P[Unit]    = P("isLiteral")
  def isNumeric[_: P]: P[Unit]    = P("isNumeric")
  def encodeForURI[_: P]: P[Unit] = P("encode_for_uri")
  def lang[_: P]: P[Unit]         = P("lang")
  def langMatches[_: P]: P[Unit]  = P("langMatches")
  def md5[_: P]: P[Unit]          = P("md5" | "MD5")
  def sha1[_: P]: P[Unit]         = P("sha1" | "SHA1")
  def sha256[_: P]: P[Unit]       = P("sha256" | "SHA256")
  def sha384[_: P]: P[Unit]       = P("sha384" | "SHA384")
  def sha512[_: P]: P[Unit]       = P("sha512" | "SHA512")
  def uuid[_: P]: P[Unit]         = P("uuid")
  def ceil[_: P]: P[Unit]         = P("ceil")
  def round[_: P]: P[Unit]        = P("round")
  def rand[_: P]: P[Unit]         = P("rand")
  def strUuid[_: P]: P[Unit]      = P("struuid")

  def uriParen[_: P]: P[URI] =
    P("(" ~ uri ~ ExpressionParser.parser ~ ")").map(s => URI(s))
  def concatParen[_: P]: P[CONCAT] =
    ("(" ~ concat ~ ExpressionParser.parser ~ ExpressionParser.parser.rep(
      1
    ) ~ ")").map { case (appendTo, append) =>
      CONCAT(appendTo, append.toList)
    }
  def strParen[_: P]: P[STR] =
    P("(" ~ str ~ ExpressionParser.parser ~ ")").map(s => STR(s))
  def strafterParen[_: P]: P[STRAFTER] = P(
    "(" ~ strafter ~ (StringValParser.litParser | BuiltInFuncParser.parser) ~ (StringValParser.litParser | BuiltInFuncParser.parser) ~ ")"
  ).map { s =>
    STRAFTER(s._1, s._2)
  }

  def strbeforeParen[_: P]: P[STRBEFORE] = P(
    "(" ~ strbefore ~ (StringValParser.litParser | BuiltInFuncParser.parser) ~ (StringValParser.litParser | BuiltInFuncParser.parser) ~ ")"
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
    P(
      "(" ~ strlen ~ ExpressionParser.parser ~ ")"
    ).map(STRLEN)

  def lcaseParen[_: P]: P[LCASE] =
    P("(" ~ lcase ~ ExpressionParser.parser ~ ")")
      .map(f => LCASE(f))

  def ucaseParen[_: P]: P[UCASE] =
    P("(" ~ ucase ~ ExpressionParser.parser ~ ")")
      .map(f => UCASE(f))

  def isLiteralParen[_: P]: P[ISLITERAL] =
    P("(" ~ isLiteral ~ ExpressionParser.parser ~ ")")
      .map(f => ISLITERAL(f))

  def isNumericParen[_: P]: P[ISNUMERIC] =
    P("(" ~ isNumeric ~ ExpressionParser.parser ~ ")")
      .map(f => ISNUMERIC(f))

  def encodeForURIParen[_: P]: P[ENCODE_FOR_URI] =
    P("(" ~ encodeForURI ~ ExpressionParser.parser ~ ")")
      .map(f => ENCODE_FOR_URI(f))

  def langParen[_: P]: P[LANG] =
    P("(" ~ lang ~ ExpressionParser.parser ~ ")")
      .map(f => LANG(f))

  def langMatchesParen[_: P]: P[LANGMATCHES] =
    P(
      "(" ~ langMatches ~ ExpressionParser.parser ~ ExpressionParser.parser ~ ")"
    )
      .map(f => LANGMATCHES(f._1, f._2))

  def md5Paren[_: P]: P[MD5] =
    P("(" ~ md5 ~ ExpressionParser.parser ~ ")")
      .map(f => MD5(f))

  def sha1Paren[_: P]: P[SHA1] =
    P("(" ~ sha1 ~ ExpressionParser.parser ~ ")")
      .map(f => SHA1(f))

  def sha256Paren[_: P]: P[SHA256] =
    P("(" ~ sha256 ~ ExpressionParser.parser ~ ")")
      .map(f => SHA256(f))

  def sha384Paren[_: P]: P[SHA384] =
    P("(" ~ sha384 ~ ExpressionParser.parser ~ ")")
      .map(f => SHA384(f))

  def sha512Paren[_: P]: P[SHA512] =
    P("(" ~ sha512 ~ ExpressionParser.parser ~ ")")
      .map(f => SHA512(f))

  def uuidParen[_: P]: P[UUID] =
    P("(" ~ uuid ~ ")")
      .map(f => UUID())

  def ceilParen[_: P]: P[CEIL] =
    P("(" ~ ceil ~ ExpressionParser.parser ~ ")")
      .map(f => CEIL(f))

  def roundParen[_: P]: P[ROUND] =
    P("(" ~ round ~ ExpressionParser.parser ~ ")")
      .map(f => ROUND(f))

  def randParen[_: P]: P[RAND] =
    P("(" ~ rand ~ ")")
      .map(f => RAND())

  def strUuidParen[_: P]: P[STRUUID] =
    P("(" ~ strUuid ~ ")")
      .map(f => STRUUID())

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
        | lcaseParen
        | ucaseParen
        | isLiteralParen
        | isNumericParen
        | encodeForURIParen
        | langParen
        | langMatchesParen
        | md5Paren
        | sha1Paren
        | sha256Paren
        | sha384Paren
        | sha512Paren
        | uuidParen
        | ceilParen
        | roundParen
        | randParen
        | strUuidParen
    )
//      | StringValParser.string
//      | StringValParser.variable)

  def parser[_: P]: P[StringLike] = P(funcPatterns)
}
