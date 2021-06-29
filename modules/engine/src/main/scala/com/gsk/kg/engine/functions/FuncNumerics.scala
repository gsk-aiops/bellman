package com.gsk.kg.engine.functions

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.format_string
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.when
import org.apache.spark.sql.functions.{abs => sAbs}
import org.apache.spark.sql.functions.{ceil => sCeil}
import org.apache.spark.sql.functions.{floor => sFloor}
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
  def abs: Column => Column = col => apply(sAbs, col)

  /** Returns the number with no fractional part that is closest to the argument.
    * If there are two such numbers, then the one that is closest to positive infinity
    * is returned. An error is raised if arg is not a numeric value.
    * @param col
    * @return
    */
  def round: Column => Column = col => apply(sRodund, col)

  /** Returns the smallest (closest to negative infinity) number with no fractional part
    * that is not less than the value of arg. An error is raised if arg is not a numeric value.
    * @param col
    * @return
    */
  def ceil: Column => Column = col => apply(sCeil, col)

  /** Returns the largest (closest to positive infinity) number with no fractional part that is not greater
    * than the value of arg. An error is raised if arg is not a numeric value.
    * @param col
    * @return
    */
  def floor: Column => Column = col => apply(sFloor, col)

  /** Returns a pseudo-random number between 0 (inclusive) and 1.0e0 (exclusive). Different numbers can be
    * produced every time this function is invoked. Numbers should be produced with approximately equal probability.
    * @return
    */
  def rand: Column = format_string("\"%s\"^^%s", sRand(), lit("xsd:double"))

  /** Apply function f over column col detecting malformed data
    * e.g.
    * case col = 10                      ==> f(10)
    * case col = "\"2\"^^xsd:int"        ==> f(2)
    * case col = "\"-0.3\"^^xsd:decimal" ==> f(-0.3)
    * case col = "\"-10.5\"^^xsd:string" ==> null, this value is incorrect
    * case col = "\"2.3\"^^xsd:int"      ==> null, this value is incorrect
    * @param f
    * @param col
    * @return
    */
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
