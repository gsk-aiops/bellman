package com.gsk.kg.sparqlparser

import com.gsk.kg.sparqlparser.DateTimeFunc._

import fastparse.MultiLineWhitespace._
import fastparse._

/** Functions on Functions on Dates and Times:
  * https://www.w3.org/TR/sparql11-query/#func-date-time
  */
object DateTimeFuncsParser {

  def now[_: P]: P[Unit]      = P("now")
  def year[_: P]: P[Unit]     = P("year")
  def month[_: P]: P[Unit]    = P("month")
  def day[_: P]: P[Unit]      = P("day")
  def hours[_: P]: P[Unit]    = P("hours")
  def minutes[_: P]: P[Unit]  = P("minutes")
  def seconds[_: P]: P[Unit]  = P("seconds")
  def timezone[_: P]: P[Unit] = P("timezone")
  def tz[_: P]: P[Unit]       = P("tz")

  def nowParen[_: P]: P[NOW] =
    P("(" ~ now ~ ")")
      .map(f => NOW())

  def yearParen[_: P]: P[YEAR] =
    P("(" ~ year ~ ExpressionParser.parser ~ ")")
      .map(f => YEAR(f))

  def monthParen[_: P]: P[MONTH] =
    P("(" ~ month ~ ExpressionParser.parser ~ ")")
      .map(f => MONTH(f))

  def dayParen[_: P]: P[DAY] =
    P("(" ~ day ~ ExpressionParser.parser ~ ")")
      .map(f => DAY(f))

  def hoursParen[_: P]: P[HOUR] =
    P("(" ~ hours ~ ExpressionParser.parser ~ ")")
      .map(f => HOUR(f))

  def minutesParen[_: P]: P[MINUTES] =
    P("(" ~ minutes ~ ExpressionParser.parser ~ ")")
      .map(f => MINUTES(f))

  def secondsParen[_: P]: P[SECONDS] =
    P("(" ~ seconds ~ ExpressionParser.parser ~ ")")
      .map(f => SECONDS(f))

  def timezoneParen[_: P]: P[TIMEZONE] =
    P("(" ~ timezone ~ ExpressionParser.parser ~ ")")
      .map(f => TIMEZONE(f))

  def tzParen[_: P]: P[TZ] =
    P("(" ~ tz ~ ExpressionParser.parser ~ ")")
      .map(f => TZ(f))

  def parser[_: P]: P[DateTimeFunc] =
    P(
      nowParen
        | yearParen
        | monthParen
        | dayParen
        | hoursParen
        | minutesParen
        | secondsParen
        | timezoneParen
        | tzParen
    )
}
