package com.gsk.kg.engine
package scalacheck

import org.scalacheck.Gen
import org.scalacheck.cats.implicits._
import cats.implicits._
import com.gsk.kg.sparqlparser._
import org.scalacheck.Arbitrary

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
    Gen.lzy(expressionGenerator).map(BuildInFunc.URI(_)),
    (Gen.lzy(expressionGenerator), Gen.lzy(expressionGenerator))
      .mapN(BuildInFunc.CONCAT(_, _)),
    Gen.lzy(expressionGenerator).map(BuildInFunc.STR(_)),
    (Gen.lzy(expressionGenerator), Gen.lzy(expressionGenerator))
      .mapN(BuildInFunc.STRAFTER(_, _)),
    (Gen.lzy(expressionGenerator), Gen.lzy(expressionGenerator))
      .mapN(BuildInFunc.STRSTARTS(_, _)),
    Gen.lzy(expressionGenerator).map(BuildInFunc.ISBLANK(_)),
    (
      Gen.lzy(expressionGenerator),
      Gen.lzy(expressionGenerator),
      Gen.lzy(expressionGenerator)
    ).mapN(BuildInFunc.REPLACE(_, _, _)),
    (Gen.lzy(expressionGenerator), Gen.lzy(expressionGenerator))
      .mapN(BuildInFunc.REGEX(_, _))
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
