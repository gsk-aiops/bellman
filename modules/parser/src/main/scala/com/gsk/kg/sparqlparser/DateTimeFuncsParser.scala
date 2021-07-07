package com.gsk.kg.sparqlparser

import com.gsk.kg.sparqlparser.DateTimeFunc._

import fastparse.MultiLineWhitespace._
import fastparse._

/** Functions on Functions on Dates and Times:
  * https://www.w3.org/TR/sparql11-query/#func-date-time
  */
object DateTimeFuncsParser {

  def now[_: P]: P[Unit]   = P("now")
  def year[_: P]: P[Unit]  = P("year")
  def month[_: P]: P[Unit] = P("month")
  def hour[_: P]: P[Unit]  = P("hour")

  def nowParen[_: P]: P[NOW] =
    P("(" ~ now ~ ")")
      .map(f => NOW())

  def yearParen[_: P]: P[YEAR] =
    P("(" ~ year ~ ExpressionParser.parser ~ ")")
      .map(f => YEAR(f))

  def monthParen[_: P]: P[MONTH] =
    P("(" ~ month ~ ExpressionParser.parser ~ ")")
      .map(f => MONTH(f))

  def hourParen[_: P]: P[HOUR] =
    P("(" ~ hour ~ ExpressionParser.parser ~ ")")
      .map(f => HOUR(f))

  def parser[_: P]: P[DateTimeFunc] =
    P(
      nowParen
        | yearParen
        | monthParen
        | hourParen
    )
}
