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
          .otherwise(
            promoteStringBoolean(l, r)(_ === _)
              .otherwise(l === r)
          )
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
          otherwise (
            promoteStringBoolean(l, r)(_ > _)
              .otherwise(l > r)
          )
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
          .otherwise(
            promoteStringBoolean(l, r)(_ < _)
              .otherwise(l < r)
          )
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
          .otherwise(
            promoteStringBoolean(l, r)(_ >= _)
              .otherwise(l >= r)
          )
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
          .otherwise(
            promoteStringBoolean(l, r)(_ <= _)
              .otherwise(l <= r)
          )
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
  def `if`(cnd: Column, ifTrue: Column, ifFalse: Column): Column = {
    val ebv = effectiveBooleanValue(cnd)
    when(
      ebv.isNotNull,
      when(
        ebv,
        ifTrue
      ).otherwise(ifFalse)
    ).otherwise(nullLiteral)
  }

  /** Effective boolean value is used to calculate the arguments to the logical functions logical-and, logical-or,
    * and fn:not, as well as evaluate the result of a FILTER expression.
    *
    * The XQuery Effective Boolean Value rules rely on the definition of XPath's fn:boolean.
    * The following rules reflect the rules for fn:boolean applied to the argument types present in SPARQL queries:
    *
    * - The EBV of any literal whose type is xsd:boolean or numeric is false if the lexical form is not valid for
    * that datatype (e.g. "abc"^^xsd:integer).
    * - If the argument is a typed literal with a datatype of xsd:boolean, and it has a valid lexical form,
    * the EBV is the value of that argument.
    * - If the argument is a plain literal or a typed literal with a datatype of xsd:string,
    * the EBV is false if the operand value has zero length; otherwise the EBV is true.
    * - If the argument is a numeric type or a typed literal with a datatype derived from a numeric type,
    * and it has a valid lexical form, the EBV is false if the operand value is NaN or is numerically equal to zero;
    * otherwise the EBV is true.
    * - All other arguments, including unbound arguments, produce a type error.
    *
    * An EBV of true is represented as a typed literal with a datatype of xsd:boolean and a lexical value of "true";
    * an EBV of false is represented as a typed literal with a datatype of xsd:boolean and a lexical value of "false".
    * @param column
    * @return
    */
  def effectiveBooleanValue(col: Column): Column = {
    val typed = TypedLiteral(col)

    lazy val ifBooleanLit = when(
      typed.value =!= lit("true") && typed.value =!= "false",
      lit(false)
    ).otherwise(
      typed.value.cast("boolean")
    )

    lazy val ifNumericLit = when(
      typed.value.cast("double").isNull,
      lit(false)
    ).otherwise {
      val double = typed.value.cast("double")
      when(
        isnan(double) || double === lit(0.0),
        lit(false)
      ).otherwise(lit(true))
    }

    lazy val ifPlainLit = when(
      typed.value === lit(""),
      lit(false)
    ).when(
      typed.value.cast("boolean").isNotNull,
      typed.value.cast("boolean")
    ).when(
      typed.value.cast("double").isNull,
      lit(true)
    ).otherwise {
      val double = typed.value.cast("double")
      when(
        isnan(double) || double === lit(0.0),
        lit(false)
      ).otherwise(lit(true))
    }

    lazy val ifStringLit = when(
      typed.value === lit(""),
      lit(false)
    ).otherwise(lit(true))

    when(
      isBooleanLiteral(col),
      ifBooleanLit
    ).when(
      isNumericLiteral(col),
      ifNumericLit
    ).when(
      typed.tag === lit(""),
      ifPlainLit
    ).when(
      isStringLiteral(col),
      ifStringLit
    ).otherwise(nullLiteral)
  }
}
