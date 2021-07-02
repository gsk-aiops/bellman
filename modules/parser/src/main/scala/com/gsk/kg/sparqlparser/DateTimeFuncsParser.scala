package com.gsk.kg.sparqlparser

import com.gsk.kg.sparqlparser.DateTimeFunc._

import fastparse.MultiLineWhitespace._
import fastparse._

/** Functions on Functions on Dates and Times:
  * https://www.w3.org/TR/sparql11-query/#func-date-time
  */
object DateTimeFuncsParser {

  def now[_: P]: P[Unit] = P("now")

  def nowParen[_: P]: P[NOW] =
    P("(" ~ now ~ ")")
      .map(f => NOW())

  def parser[_: P]: P[DateTimeFunc] =
    P(
      nowParen
    )
}
