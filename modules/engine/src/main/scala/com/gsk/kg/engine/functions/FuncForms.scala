package com.gsk.kg.engine.functions

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

import com.gsk.kg.engine.RdfFormatter
import com.gsk.kg.engine.functions.Literals._

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

  /** Returns TRUE if term1 and term2 are the same RDF term
    * @param l
    * @param r
    * @return
    */
  def sameTerm(l: Column, r: Column): Column = {

    val leftAndRightLocalized = {
      val lLocalized = LocalizedLiteral(l)
      val rLocalized = LocalizedLiteral(r)
      when(
        lLocalized.tag === rLocalized.tag,
        lLocalized.value === rLocalized.value
      ).otherwise(lit(false))
    }

    val leftLocalized = {
      val lLocalized = LocalizedLiteral(l)
      lLocalized.value === r
    }

    val rightLocalized = {
      val rLocalized = LocalizedLiteral(r)
      l === rLocalized.value
    }

    val leftAndRightTyped = {
      val lDataTyped = TypedLiteral(l)
      val rDataTyped = TypedLiteral(r)
      when(
        lDataTyped.tag === rDataTyped.tag,
        lDataTyped.value === rDataTyped.value
      ).otherwise(lit(false))
    }

    val leftTyped = {
      val lTyped = TypedLiteral(l)
      lTyped.value === r
    }

    val rightTyped = {
      val rTyped = TypedLiteral(r)
      l === rTyped.value
    }

    when(
      RdfFormatter.isLocalizedString(l) && RdfFormatter.isLocalizedString(r),
      leftAndRightLocalized
    ).when(
      RdfFormatter.isLocalizedString(l),
      leftLocalized
    ).when(
      RdfFormatter.isLocalizedString(r),
      rightLocalized
    ).when(
      RdfFormatter.isDatatypeLiteral(l) && RdfFormatter.isDatatypeLiteral(r),
      leftAndRightTyped
    ).when(
      RdfFormatter.isDatatypeLiteral(l),
      leftTyped
    ).when(
      RdfFormatter.isDatatypeLiteral(r),
      rightTyped
    ).otherwise(l === r)
  }

  /** The IF function form evaluates the first argument, interprets it as a effective boolean value,
    * then returns the value of expression2 if the EBV is true, otherwise it returns the value of expression3.
    * Only one of expression2 and expression3 is evaluated. If evaluating the first argument raises an error,
    * then an error is raised for the evaluation of the IF expression.
    * @param cnd
    * @param ifTrue
    * @param ifFalse
    * @return
    */
  def `if`(cnd: Column, ifTrue: Column, ifFalse: Column): Column = ???
}
