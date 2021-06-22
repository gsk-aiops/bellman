package com.gsk.kg.engine.functions

import com.gsk.kg.engine.functions.Literals.NumericLiteral
import com.gsk.kg.engine.functions.Literals.isNumericLiteral
import com.gsk.kg.engine.functions.Literals.nullLiteral
import com.gsk.kg.engine.functions.Literals.promoteBooleanBoolean
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.when
import org.apache.spark.sql.functions.{ceil => sCeil}

object FuncNumerics {

  /** Returns the absolute value of arg. An error is raised if arg is not a numeric value.
    * @param col
    * @return
    */
  def abs(col: Column): Column = ???

  /** Returns the number with no fractional part that is closest to the argument.
    * If there are two such numbers, then the one that is closest to positive infinity
    * is returned. An error is raised if arg is not a numeric value.
    * @param col
    * @return
    */
  def round(col: Column): Column = ???

  /** Returns the smallest (closest to negative infinity) number with no fractional part
    * that is not less than the value of arg. An error is raised if arg is not a numeric value.
    * @param col
    * @return
    */
  def ceil(col: Column): Column = {
    when(
      isNumericLiteral(col), {
        val n = NumericLiteral(col).value
        sCeil(n)
      }
    ).otherwise(nullLiteral)
  }

  /** Returns the largest (closest to positive infinity) number with no fractional part that is not greater
    * than the value of arg. An error is raised if arg is not a numeric value.
    * @param col
    * @return
    */
  def floor(col: Column): Column = ???

  /** Returns a pseudo-random number between 0 (inclusive) and 1.0e0 (exclusive). Different numbers can be
    * produced every time this function is invoked. Numbers should be produced with approximately equal probability.
    * @return
    */
  def rand: Column = ???
}
