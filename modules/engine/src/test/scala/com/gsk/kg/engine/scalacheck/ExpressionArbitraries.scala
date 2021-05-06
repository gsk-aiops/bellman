package com.gsk.kg.engine
package scalacheck

import cats.implicits._

import com.gsk.kg.sparqlparser._

import org.scalacheck.Arbitrary
import org.scalacheck.Gen
import org.scalacheck.cats.implicits._

trait ExpressionArbitraries extends CommonGenerators {

  val expressionGenerator: Gen[Expression] =
    Gen.lzy(
      Gen.frequency(
        7 -> stringValGenerator,
        1 -> builtinFuncGenerator,
        1 -> conditionalGenerator
      )
    )

  val stringValGenerator: Gen[StringVal] = Gen.oneOf(
    (nonEmptyStringGenerator, Gen.option(sparqlDataTypesGen))
      .mapN(StringVal.STRING(_, _)),
    Gen.numStr.map(StringVal.NUM(_)),
    nonEmptyStringGenerator.map(str => StringVal.VARIABLE(s"?$str")),
    nonEmptyStringGenerator.map(uri => StringVal.URIVAL(uri)),
    nonEmptyStringGenerator.map(StringVal.BLANK(_)),
    Gen.oneOf(true, false).map(bool => StringVal.BOOL(bool.toString))
  )

  val builtinFuncGenerator: Gen[Expression] = Gen.oneOf(
    Gen.lzy(expressionGenerator).map(BuiltInFunc.URI(_)),
    (Gen.lzy(expressionGenerator), Gen.lzy(expressionGenerator))
      .mapN(BuiltInFunc.CONCAT(_, _)),
    Gen.lzy(expressionGenerator).map(BuiltInFunc.STR(_)),
    (Gen.lzy(expressionGenerator), Gen.lzy(expressionGenerator))
      .mapN(BuiltInFunc.STRENDS(_, _)),
    (Gen.lzy(expressionGenerator), Gen.lzy(expressionGenerator))
      .mapN(BuiltInFunc.STRAFTER(_, _)),
    (Gen.lzy(expressionGenerator), Gen.lzy(expressionGenerator))
      .mapN(BuiltInFunc.STRBEFORE(_, _)),
    Gen.lzy(expressionGenerator).map(BuiltInFunc.STRLEN(_)),
    (Gen.lzy(expressionGenerator), Gen.lzy(expressionGenerator))
      .mapN(BuiltInFunc.SUBSTR(_, _)),
    (Gen.lzy(expressionGenerator), Gen.lzy(expressionGenerator))
      .mapN(BuiltInFunc.STRSTARTS(_, _)),
    Gen.lzy(expressionGenerator).map(BuiltInFunc.ISBLANK(_)),
    (
      Gen.lzy(expressionGenerator),
      Gen.lzy(expressionGenerator),
      Gen.lzy(expressionGenerator)
    ).mapN(BuiltInFunc.REPLACE(_, _, _)),
    (Gen.lzy(expressionGenerator), Gen.lzy(expressionGenerator))
      .mapN(BuiltInFunc.REGEX(_, _))
  )

  val conditionalGenerator: Gen[Expression] = Gen.oneOf(
    (Gen.lzy(expressionGenerator), Gen.lzy(expressionGenerator))
      .mapN(Conditional.EQUALS(_, _)),
    (Gen.lzy(expressionGenerator), Gen.lzy(expressionGenerator))
      .mapN(Conditional.NOTEQUALS(_, _)),
    (Gen.lzy(expressionGenerator), Gen.lzy(expressionGenerator))
      .mapN(Conditional.GT(_, _)),
    (Gen.lzy(expressionGenerator), Gen.lzy(expressionGenerator))
      .mapN(Conditional.GTE(_, _)),
    (Gen.lzy(expressionGenerator), Gen.lzy(expressionGenerator))
      .mapN(Conditional.LT(_, _)),
    (Gen.lzy(expressionGenerator), Gen.lzy(expressionGenerator))
      .mapN(Conditional.LTE(_, _)),
    (Gen.lzy(expressionGenerator), Gen.lzy(expressionGenerator))
      .mapN(Conditional.OR(_, _)),
    (Gen.lzy(expressionGenerator), Gen.lzy(expressionGenerator))
      .mapN(Conditional.AND(_, _)),
    Gen.lzy(expressionGenerator).map(Conditional.NEGATE(_))
  )

  implicit val arbitraryExpression: Arbitrary[Expression] = Arbitrary(
    Gen.lzy(expressionGenerator)
  )
}

object ExpressionArbitraries extends ExpressionArbitraries
