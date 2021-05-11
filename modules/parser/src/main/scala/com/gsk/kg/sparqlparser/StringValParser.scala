package com.gsk.kg.sparqlparser

import com.gsk.kg.sparqlparser.StringVal._

import fastparse.MultiLineWhitespace._
import fastparse._

object StringValParser {
  def lang[_: P]: P[String] = P("@" ~ CharsWhileIn("a-zA-Z")).!
  def iri[_: P]: P[String]  = P("<" ~ (CharsWhile(_ != '>')) ~ ">").!
  // RDF string literal could have optional language or optional iri

  def langString[_: P]: P[LANG_STRING] = P(
    "\"" ~ CharsWhile(_ != '\"').! ~ "\"" ~ lang
  ).map { case (s, tag) =>
    LANG_STRING(s, tag)
  }

  def dataTypeString[_: P]: P[DT_STRING] = P(
    "\"" ~ CharsWhile(_ != '\"').! ~ "\"" ~ "^^" ~ iri
  ).map { case (s, tag) =>
    DT_STRING(s, tag)
  }

  def plainString[_: P]: P[STRING] = P(
    "\"" ~ CharsWhile(_ != '\"').! ~ "\""
  ).map(s => STRING(s))

  //TODO improve regex. Include all valid sparql varnames in spec: https://www.w3.org/TR/sparql11-query/#rPN_CHARS_BASE
  def variable[_: P]: P[VARIABLE] =
    P("?" ~ "?".? ~ CharsWhileIn("a-zA-Z0-9_.")).!.map(str =>
      VARIABLE(str.replace(".", ""))
    )
  def urival[_: P]: P[URIVAL] =
    iri.map(URIVAL)
  def num[_: P]: P[NUM] =
    P("-".? ~ CharsWhileIn("0-9") ~ ("." ~ CharsWhileIn("0-9")).?).!.map {
      NUM
    } //with optional decimals
  def blankNode[_: P]: P[BLANK] =
    P("_:" ~ CharsWhileIn("a-zA-Z0-9_").!).map(BLANK(_))
  def bool[_: P]: P[BOOL] = P("true" | "false").!.map(BOOL)
  def optionLong[_: P]: P[Option[Long]] = P(CharsWhileIn("0-9_")).!.map { s =>
    if (s.contains('_')) None else Some(s.toLong)
  }

  def tripleValParser[_: P]: P[StringVal] = P(
    variable | urival | langString | dataTypeString | plainString | num | blankNode | bool
  )
}
