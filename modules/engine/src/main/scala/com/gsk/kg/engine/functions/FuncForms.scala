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
        promoteNumericArgsToBooleanResult(l, r)(_ === _)
          .otherwise(
            promoteStringArgsToBooleanResult(l, r)(_ === _)
              .otherwise(
                promoteBooleanBooleanToBooleanResult(l, r)(_ === _)
                  .otherwise(l === r)
              )
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
        promoteNumericArgsToBooleanResult(l, r)(_ > _)
          otherwise (
            promoteStringArgsToBooleanResult(l, r)(_ > _)
              .otherwise(
                promoteBooleanBooleanToBooleanResult(l, r)(_ > _)
                  .otherwise(l > r)
              )
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
        promoteNumericArgsToBooleanResult(l, r)(_ < _)
          .otherwise(
            promoteStringArgsToBooleanResult(l, r)(_ < _)
              .otherwise(
                promoteBooleanBooleanToBooleanResult(l, r)(_ < _)
                  .otherwise(l < r)
              )
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
        promoteNumericArgsToBooleanResult(l, r)(_ >= _)
          .otherwise(
            promoteStringArgsToBooleanResult(l, r)(_ >= _)
              .otherwise(
                promoteBooleanBooleanToBooleanResult(l, r)(_ >= _)
                  .otherwise(l >= r)
              )
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
        promoteNumericArgsToBooleanResult(l, r)(_ <= _)
          .otherwise(
            promoteStringArgsToBooleanResult(l, r)(_ <= _)
              .otherwise(
                promoteBooleanBooleanToBooleanResult(l, r)(_ <= _)
                  .otherwise(l <= r)
              )
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

    val anyEqualsExpr = xs.foldLeft(FalseCol) { case (acc, x) =>
      acc || (e === x)
    }

    val anyIsNull = xs.foldLeft(FalseCol) { case (acc, x) =>
      acc || x.isNull
    }

    when(
      anyEqualsExpr,
      TrueCol
    ).otherwise(
      when(
        anyIsNull,
        Literals.nullLiteral
      ).otherwise(FalseCol)
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
      ).otherwise(FalseCol)
    }

    /** Return if two type of columns are similar
      * @param l
      * @param r
      * @return
      */
    def isSameTag(l: Column, r: Column): Column = {
      val True = TrueCol
      when(isStringLiteral(l) && isStringLiteral(r), True)
        .when(isBooleanLiteral(l) && isBooleanLiteral(r), True)
        .otherwise(TypedLiteral(l).tag === TypedLiteral(r).tag)
    }

    def leftAndRightTyped(left: Column, right: Column): Column = {
      val lDataTyped = TypedLiteral(left)
      val rDataTyped = TypedLiteral(right)
      when(
        isSameTag(left, right),
        lDataTyped.value === rDataTyped.value
      ).otherwise(FalseCol)
    }

    when(
      RdfFormatter.isLocalizedString(l) && RdfFormatter.isLocalizedString(r),
      leftAndRightLocalized
    )
      .when(
        RdfFormatter.isDatatypeLiteral(l) && RdfFormatter.isDatatypeLiteral(r),
        leftAndRightTyped(l, r)
      )
      .when(
        RdfFormatter.isDatatypeLiteral(l),
        leftAndRightTyped(l, Literals.inferType(r))
      )
      .when(
        RdfFormatter.isDatatypeLiteral(r),
        leftAndRightTyped(Literals.inferType(l), r)
      )
      .otherwise(Literals.inferType(l) === Literals.inferType(r))
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
      FalseCol
    ).otherwise(
      typed.value.cast("boolean")
    )

    lazy val ifNumericLit = when(
      typed.value.cast("double").isNull,
      FalseCol
    ).otherwise {
      val double = typed.value.cast("double")
      when(
        isnan(double) || double === lit(0.0),
        FalseCol
      ).otherwise(TrueCol)
    }

    lazy val ifPlainLit = when(
      typed.value === EmptyStringCol,
      FalseCol
    ).when(
      typed.value.cast("boolean").isNotNull,
      typed.value.cast("boolean")
    ).when(
      typed.value.cast("double").isNull,
      TrueCol
    ).otherwise {
      val double = typed.value.cast("double")
      when(
        isnan(double) || double === lit(0.0),
        FalseCol
      ).otherwise(TrueCol)
    }

    lazy val ifStringLit = when(
      typed.value === EmptyStringCol,
      FalseCol
    ).otherwise(TrueCol)

    when(
      isBooleanLiteral(col),
      ifBooleanLit
    ).when(
      isNumericLiteral(col),
      ifNumericLit
    ).when(
      typed.tag === EmptyStringCol,
      ifPlainLit
    ).when(
      isStringLiteral(col),
      ifStringLit
    ).otherwise(nullLiteral)
  }

  /** Returns true if var is bound to a value. Returns false otherwise. Variables with the value NaN or INF
    * are considered bound.
    * @param c
    * @return
    */
  def bound(c: Column): Column =
    c.isNotNull

  /** The COALESCE function form returns the RDF term value of the first expression that evaluates without error.
    * In SPARQL, evaluating an unbound variable raises an error.
    * If none of the arguments evaluates to an RDF term, an error is raised.
    * If no expressions are evaluated without error, an error is raised.
    * @param cols
    * @return
    */
  def coalesce(cols: List[Column]): Column = {
    cols.foldLeft(nullLiteral) { case (acc, elem) =>
      when(
        elem.isNull,
        acc
      ).otherwise(
        when(
          acc.isNull,
          elem
        ).otherwise(acc)
      )
    }
  }
}
