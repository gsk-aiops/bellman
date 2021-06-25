package com.gsk.kg.engine.functions

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.format_string
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.when
import org.apache.spark.sql.functions.{ceil => sCeil}
import org.apache.spark.sql.functions.{rand => sRand}
import org.apache.spark.sql.functions.{round => sRodund}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.IntegerType
import com.gsk.kg.engine.functions.Literals.NumericLiteral
import com.gsk.kg.engine.functions.Literals.isIntNumericLiteral
import com.gsk.kg.engine.functions.Literals.isNumericLiteral
import com.gsk.kg.engine.functions.Literals.isPlainLiteral
import com.gsk.kg.engine.functions.Literals.nullLiteral

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
  def round(col: Column): Column = apply(sRodund, col)

  /** Returns the smallest (closest to negative infinity) number with no fractional part
    * that is not less than the value of arg. An error is raised if arg is not a numeric value.
    * @param col
    * @return
    */
  def ceil(col: Column): Column = apply(sCeil, col)

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
  def rand: Column = format_string("\"%s\"^^%s", sRand(), lit("xsd:double"))

  private def apply(f: Column => Column, col: Column): Column =
    when(
      isPlainLiteral(col) && col.cast(DoubleType).isNotNull,
      f(col)
    ).when(
      isNumericLiteral(col), {
        val numericLiteral = NumericLiteral(col)
        val n              = numericLiteral.value
        val tag            = numericLiteral.tag
        when(
          isIntNumericLiteral(col) && n.cast(IntegerType).isNotNull && !n
            .contains("."),
          format_string("\"%s\"^^%s", f(n).cast(IntegerType), tag)
        ).when(
          isIntNumericLiteral(col) && n.cast(IntegerType).isNotNull && n
            .contains("."),
          nullLiteral
        ).otherwise(format_string("\"%s\"^^%s", f(n), tag))
      }
    ).otherwise(nullLiteral)
}
