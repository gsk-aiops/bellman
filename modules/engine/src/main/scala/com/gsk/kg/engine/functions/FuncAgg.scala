package com.gsk.kg.engine.functions

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

import com.gsk.kg.engine.RdfFormatter
import com.gsk.kg.engine.functions.Literals.NumericLiteral
import com.gsk.kg.engine.functions.Literals.TypedLiteral
import com.gsk.kg.engine.functions.Literals.isNumericLiteral
import com.gsk.kg.engine.functions.Literals.isPlainLiteral
import com.gsk.kg.engine.functions.Literals.nullLiteral

object FuncAgg {

  /** Sample is a set function which returns an arbitrary value from
    * the multiset passed to it.
    *
    * Implemented using [[org.apache.spark.sql.functions.first]].
    *
    * @param col
    * @return
    */
  def sample(col: Column): Column =
    first(col, true)

  /** This functions count the number of elements in a group
    * @param col
    * @return
    */
  def countAgg(col: Column): Column =
    count(col)

  /** This function calculates the average on a numeric type group
    * @param col
    * @return
    */
  def avgAgg(col: Column): Column = {
    avg(
      when(
        isNumericLiteral(col), {
          val numeric = NumericLiteral(col)
          numeric.value
        }
      ).when(
        isPlainLiteral(col) && col.cast("double").isNotNull,
        col
      ).otherwise(nullLiteral)
    )
  }

  /** This function calculates the sum on a numeric type group
    * @param col
    * @return
    */
  def sumAgg(col: Column): Column = {
    sum(
      when(
        isNumericLiteral(col), {
          val numeric = NumericLiteral(col)
          numeric.value
        }
      ).when(
        isPlainLiteral(col) && col.cast("double").isNotNull,
        col
      ).otherwise(nullLiteral)
    )
  }

  /** This function calculates the minimum of a group when numeric or other literals like strings
    * @param col
    * @return
    */
  def minAgg(col: Column): Column = {
    min(
      when(
        isNumericLiteral(col), {
          val numeric = NumericLiteral(col)
          numeric.value
        }
      ).when(
        RdfFormatter.isDatatypeLiteral(col), {
          val typed = TypedLiteral(col)
          regexp_replace(typed.value, "\"", "")
        }
      ).otherwise(col)
    )
  }

  /** This funciton calculates the maximum of a group when numeric or other literals like strings
    * @param col
    * @return
    */
  def maxAgg(col: Column): Column = {
    max(
      when(
        isNumericLiteral(col), {
          val numeric = NumericLiteral(col)
          numeric.value
        }
      ).when(
        RdfFormatter.isDatatypeLiteral(col), {
          val typed = TypedLiteral(col)
          regexp_replace(typed.value, "\"", "")
        }
      ).otherwise(col)
    )
  }

  // TODO: Implement group-concat https://github.com/gsk-aiops/bellman/issues/202
  def groupConcat(col: Column, separator: String): Column =
    ???
}
