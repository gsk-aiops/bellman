package com.gsk.kg.engine.functions

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.current_timestamp
import org.apache.spark.sql.functions.date_format
import org.apache.spark.sql.functions.dayofmonth
import org.apache.spark.sql.functions.format_string
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.regexp_replace
import org.apache.spark.sql.functions.split
import org.apache.spark.sql.functions.substring
import org.apache.spark.sql.functions.when
import org.apache.spark.sql.functions.{month => sMonth}
import org.apache.spark.sql.functions.{year => sYear}
import org.apache.spark.sql.types.IntegerType

import com.gsk.kg.engine.functions.Literals.NumericLiteral
import com.gsk.kg.engine.functions.Literals.isDateTimeLiteral
import com.gsk.kg.engine.functions.Literals.nullLiteral

object FuncDates {

  /** Returns an XSD dateTime value for the current query execution. All calls to this function in any one query
    * execution must return the same value. The exact moment returned is not specified.
    * e.g. "2011-01-10T14:45:13.815-05:00"^^xsd:dateTime
    * @return
    */
  def now: Column = {
    format_string(
      "\"%s\"^^xsd:dateTime",
      date_format(current_timestamp, "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
    )
  }

  /** Returns the year part of arg as an integer.
    * @param col
    * @return
    */
  def year(col: Column): Column = apply(sYear, col)

  /** Returns the month part of arg as an integer.
    * @param col
    * @return
    */
  def month(col: Column): Column = apply(sMonth, col)

  /** Returns the day part of arg as an integer.
    * @param col
    * @return
    */
  def day(col: Column): Column = apply(dayofmonth, col)

  /** Returns the hours part of arg as an integer.
    * The value is as given in the lexical form of the XSD dateTime.
    * @param col
    * @return
    */
  def hours(col: Column): Column = {
    val Hours = 3
    getTimeFromDateTimeCol(col, Hours)
  }

  /** Returns the minutes part of the lexical form of arg.
    * The value is as given in the lexical form of the XSD dateTime.
    * @param col
    * @return
    */
  def minutes(col: Column): Column = {
    val Minutes = 4
    getTimeFromDateTimeCol(col, Minutes)
  }

  /** Returns the seconds part of the lexical form of arg.
    * @param col
    * @return
    */
  def seconds(col: Column): Column = ???

  /** Returns the timezone part of arg as an xsd:dayTimeDuration.
    * Raises an error if there is no timezone.
    * @param col
    * @return
    */
  def timezone(col: Column): Column = {

    val dateTimeWithTimeZoneRegex: String =
      "[0-9]{1,4}-[0-9]{1,2}-[0-9]{1,2}T[0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}.[0-9]{1,3}[+-]{1}[0-9]{1,2}:[0-9]{1,2}"
    val dateTimeWithTimeZoneWithoutDecimalSecondsRegex: String =
      "[0-9]{1,4}-[0-9]{1,2}-[0-9]{1,2}T[0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}[+-]{1}[0-9]{1,2}:[0-9]{1,2}"
    val dateTimeWithoutTimeZoneRegex: String =
      "[0-9]{1,4}-[0-9]{1,2}-[0-9]{1,2}T[0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}.[0-9]{1,3}Z"

    val PosTimeZone     = -6
    val PosSign         = 1
    val PosHours        = 2
    val PosMinutes      = 5
    val LenTimeZone     = 6
    val LenSign         = 1
    val LenHoursMinutes = 2

    when(
      col.rlike(dateTimeWithTimeZoneRegex) || col.rlike(
        dateTimeWithTimeZoneWithoutDecimalSecondsRegex
      ), {
        val timeZone = substring(
          NumericLiteral(col).value,
          PosTimeZone,
          LenTimeZone
        )
        val sign            = substring(timeZone, PosSign, LenSign)
        val hoursTimeZone   = substring(timeZone, PosHours, LenHoursMinutes)
        val minutesTimeZone = substring(timeZone, PosMinutes, LenHoursMinutes)
        when(
          sign.like("-"),
          format_string(
            "\"%sPT%sH%sM\"^^xsd:dateTime",
            sign,
            hoursTimeZone,
            minutesTimeZone
          )
        )
          .otherwise(
            format_string(
              "\"PT%sH%sM\"^^xsd:dateTime",
              hoursTimeZone,
              minutesTimeZone
            )
          )
      }
    ).when(
      col.rlike(dateTimeWithoutTimeZoneRegex),
      lit("\"PT0S\"^^xsd:dateTime")
    ).otherwise(nullLiteral)
  }

  /** Returns the timezone part of arg as a simple literal.
    * Returns the empty string if there is no timezone.
    * @param col
    * @return
    */
  def tz(col: Column): Column = ???

  /** Check if col is a xsd:dateTime type and apply function in case true
    * @param f
    * @param col
    * @return f(col) or lit(null) if col isn't xsd:dateTime type
    */
  private def apply(f: Column => Column, col: Column): Column =
    when(
      isDateTimeLiteral(col),
      f(NumericLiteral(col).value)
    ).otherwise(nullLiteral)

  /** Get hours, minutes of dateTime column
    * @param col
    * @param pos
    * @return Column with
    *         Integer if hours or minutes
    */
  private def getTimeFromDateTimeCol(col: Column, pos: Int): Column = {
    val dateTimeRegex: String =
      "[0-9]{1,4}-[0-9]{1,2}-[0-9]{1,2}T[0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}"

    when(
      col.rlike(dateTimeRegex),
      split(
        regexp_replace(
          NumericLiteral(col).value,
          "[:TZ]",
          "-"
        ),
        "-"
      ).getItem(pos).cast(IntegerType)
    ).otherwise(nullLiteral)
  }
}
