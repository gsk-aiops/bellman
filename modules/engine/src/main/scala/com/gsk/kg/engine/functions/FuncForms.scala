package com.gsk.kg.engine.functions

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

import com.gsk.kg.engine.functions.Literals.DateLiteral
import com.gsk.kg.engine.functions.Literals.promoteNumericBoolean

object FuncForms {

  /** Performs logical binary operation '==' over two columns
    * @param l
    * @param r
    * @return
    */
  def equals(l: Column, r: Column): Column = {
    DateLiteral
      .applyDateTimeLiteral(l, r)(_ === _)
      .otherwise(
        promoteNumericBoolean(l, r)(_ === _)
          .otherwise(l === r)
      )
  }

  /** Peforms logical binary operation '>' over two columns
    * @param l
    * @param r
    * @return
    */
  def gt(l: Column, r: Column): Column =
    DateLiteral
      .applyDateTimeLiteral(l, r)(_ > _)
      .otherwise(
        promoteNumericBoolean(l, r)(_ > _)
          .otherwise(l > r)
      )

  /** Performs logical binary operation '<' over two columns
    * @param l
    * @param r
    * @return
    */
  def lt(l: Column, r: Column): Column =
    DateLiteral
      .applyDateTimeLiteral(l, r)(_ < _)
      .otherwise(
        promoteNumericBoolean(l, r)(_ < _)
          .otherwise(l < r)
      )

  /** Performs logical binary operation '<=' over two columns
    * @param l
    * @param r
    * @return
    */
  def gte(l: Column, r: Column): Column =
    DateLiteral
      .applyDateTimeLiteral(l, r)(_ >= _)
      .otherwise(
        promoteNumericBoolean(l, r)(_ >= _)
          .otherwise(l >= r)
      )

  /** Performs logical binary operation '>=' over two columns
    * @param l
    * @param r
    * @return
    */
  def lte(l: Column, r: Column): Column =
    DateLiteral
      .applyDateTimeLiteral(l, r)(_ <= _)
      .otherwise(
        promoteNumericBoolean(l, r)(_ <= _)
          .otherwise(l <= r)
      )

  /** Performs logical binary operation 'or' over two columns
    * @param l
    * @param r
    * @return
    */
  def or(l: Column, r: Column): Column =
    l || r

  /** Performs logical binary operation 'and' over two columns
    * @param r
    * @param l
    * @return
    */
  def and(l: Column, r: Column): Column =
    l && r

  /** Negates all rows of a column
    * @param s
    * @return
    */
  def negate(s: Column): Column =
    not(s)

  /** The IN operator tests whether the RDF term on the left-hand side is found in the values of list of expressions
    * on the right-hand side. The test is done with "=" operator, which tests for the same value, as determined by
    * the operator mapping.
    *
    * A list of zero terms on the right-hand side is legal.
    *
    * Errors in comparisons cause the IN expression to raise an error if the RDF term being tested is not found
    * elsewhere in the list of terms.
    * @param e
    * @param xs
    * @return
    */
  def in(e: Column, xs: List[Column]): Column = {

    val anyEqualsExpr = xs.foldLeft(lit(false)) { case (acc, x) =>
      acc || (e === x)
    }

    val anyIsNull = xs.foldLeft(lit(false)) { case (acc, x) =>
      acc || x.isNull
    }

    when(
      anyEqualsExpr,
      lit(true)
    ).otherwise(
      when(
        anyIsNull,
        Literals.nullLiteral
      ).otherwise(lit(false))
    )
  }
}
