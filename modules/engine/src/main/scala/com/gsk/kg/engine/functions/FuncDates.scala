package com.gsk.kg.engine.functions

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.current_timestamp
import org.apache.spark.sql.functions.date_format
import org.apache.spark.sql.functions.format_string

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
  def year(col: Column): Column = ???

  /** Returns the month part of arg as an integer.
    * @param col
    * @return
    */
  def month(col: Column): Column = ???

  /** Returns the day part of arg as an integer.
    * @param col
    * @return
    */
  def day(col: Column): Column = ???

  /** Returns the hours part of arg as an integer.
    * The value is as given in the lexical form of the XSD dateTime.
    * @param col
    * @return
    */
  def hours(col: Column): Column = ???

  /** Returns the minutes part of the lexical form of arg.
    * The value is as given in the lexical form of the XSD dateTime.
    * @param col
    * @return
    */
  def minutes(col: Column): Column = ???

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
  def timezone(col: Column): Column = ???

  /** Returns the timezone part of arg as a simple literal.
    * Returns the empty string if there is no timezone.
    * @param col
    * @return
    */
  def tz(col: Column): Column = ???
}
