package com.gsk.kg.engine
package scalacheck

import cats.implicits._

import com.gsk.kg.sparqlparser.StringVal.URIVAL
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
        1 -> funcFormsGenerator
      )
    )

  val uriValGenerator: Gen[URIVAL] =
    nonEmptyStringGenerator.map(StringVal.URIVAL)

  val stringValGenerator: Gen[StringVal] = Gen.oneOf(
    nonEmptyStringGenerator
      .map(StringVal.STRING),
    (
      nonEmptyStringGenerator,
      sparqlDataTypesGen
    ).mapN(StringVal.DT_STRING(_, _)),
    (
      nonEmptyStringGenerator,
      nonEmptyStringGenerator.map(x => s"@$x")
    ).mapN(StringVal.LANG_STRING(_, _)),
    Gen.numStr.map(StringVal.NUM),
    nonEmptyStringGenerator
      .map(str => StringVal.VARIABLE(s"?$str")),
    uriValGenerator,
    nonEmptyStringGenerator
      .map(StringVal.BLANK),
    Gen
      .oneOf(true, false)
      .map(bool => StringVal.BOOL(bool.toString))
  )

  val funcHashGenerator: Gen[Expression] = Gen.oneOf(
    Gen
      .lzy(expressionGenerator)
      .map(BuiltInFunc.MD5),
    Gen
      .lzy(expressionGenerator)
      .map(BuiltInFunc.SHA1),
    Gen
      .lzy(expressionGenerator)
      .map(BuiltInFunc.SHA256),
    Gen
      .lzy(expressionGenerator)
      .map(BuiltInFunc.SHA384),
    Gen
      .lzy(expressionGenerator)
      .map(BuiltInFunc.SHA512)
  )

  val funcTermsGenerator: Gen[Expression] = Gen.oneOf(
    Gen
      .lzy(expressionGenerator)
      .map(BuiltInFunc.STR),
    (
      Gen.lzy(expressionGenerator),
      Gen.lzy(uriValGenerator)
    ).mapN(BuiltInFunc.STRDT),
    Gen
      .lzy(expressionGenerator)
      .map(BuiltInFunc.URI),
    Gen
      .lzy(expressionGenerator)
      .map(BuiltInFunc.LANG),
    Gen
      .lzy(expressionGenerator)
      .map(BuiltInFunc.ISBLANK),
    Gen
      .lzy(expressionGenerator)
      .map(BuiltInFunc.ISNUMERIC),
    Gen
      .lzy(expressionGenerator)
      .map(BuiltInFunc.ISLITERAL)
  )

  val funcStringsGenerator: Gen[Expression] = Gen.oneOf(
    Gen
      .lzy(expressionGenerator)
      .map(BuiltInFunc.STRLEN),
    (
      Gen.lzy(expressionGenerator),
      Gen.lzy(expressionGenerator),
      Gen.lzy(Gen.option(expressionGenerator))
    ).mapN(BuiltInFunc.SUBSTR),
    Gen
      .lzy(expressionGenerator)
      .map(BuiltInFunc.UCASE),
    Gen
      .lzy(expressionGenerator)
      .map(BuiltInFunc.LCASE),
    (
      Gen.lzy(expressionGenerator),
      Gen.lzy(expressionGenerator)
    ).mapN(BuiltInFunc.STRSTARTS),
    (
      Gen.lzy(expressionGenerator),
      Gen.lzy(expressionGenerator)
    ).mapN(BuiltInFunc.STRENDS),
    (
      Gen.lzy(expressionGenerator),
      Gen.lzy(expressionGenerator)
    ).mapN(BuiltInFunc.STRBEFORE),
    (
      Gen.lzy(expressionGenerator),
      Gen.lzy(expressionGenerator)
    ).mapN(BuiltInFunc.STRAFTER),
    Gen
      .lzy(expressionGenerator)
      .map(BuiltInFunc.ENCODE_FOR_URI),
    (
      Gen.lzy(expressionGenerator),
      smallNonEmptyListOf(expressionGenerator)
    ).mapN(BuiltInFunc.CONCAT),
    (
      Gen.lzy(expressionGenerator),
      Gen.lzy(expressionGenerator)
    ).mapN(BuiltInFunc.LANGMATCHES),
    (
      Gen.lzy(expressionGenerator),
      Gen.lzy(expressionGenerator),
      Gen.lzy(expressionGenerator)
    ).mapN(BuiltInFunc.REGEX),
    (
      Gen.lzy(expressionGenerator),
      Gen.lzy(expressionGenerator),
      Gen.lzy(expressionGenerator),
      Gen.lzy(expressionGenerator)
    ).mapN(BuiltInFunc.REPLACE)
  )

  val funcFormsGenerator: Gen[Expression] = Gen.oneOf(
    (
      Gen.lzy(expressionGenerator),
      Gen.lzy(expressionGenerator)
    ).mapN(Conditional.EQUALS),
    (
      Gen.lzy(expressionGenerator),
      Gen.lzy(expressionGenerator)
    ).mapN(Conditional.GT),
    (
      Gen.lzy(expressionGenerator),
      Gen.lzy(expressionGenerator)
    ).mapN(Conditional.GTE),
    (
      Gen.lzy(expressionGenerator),
      Gen.lzy(expressionGenerator)
    ).mapN(Conditional.LT),
    (
      Gen.lzy(expressionGenerator),
      Gen.lzy(expressionGenerator)
    ).mapN(Conditional.LTE),
    (
      Gen.lzy(expressionGenerator),
      Gen.lzy(expressionGenerator)
    ).mapN(Conditional.OR),
    (
      Gen.lzy(expressionGenerator),
      Gen.lzy(expressionGenerator)
    ).mapN(Conditional.AND),
    Gen
      .lzy(expressionGenerator)
      .map(Conditional.NEGATE),
    (
      Gen.lzy(expressionGenerator),
      Gen.lzy(smallListOf(expressionGenerator))
    ).mapN(Conditional.IN),
    (
      Gen.lzy(expressionGenerator),
      Gen.lzy(expressionGenerator)
    ).mapN(Conditional.SAMETERM),
    (
      Gen.lzy(expressionGenerator),
      Gen.lzy(expressionGenerator),
      Gen.lzy(expressionGenerator)
    ).mapN(Conditional.IF)
  )

  val builtinFuncGenerator: Gen[Expression] = Gen.oneOf(
    funcHashGenerator,
    funcTermsGenerator,
    funcStringsGenerator,
    funcFormsGenerator
  )

  implicit val arbitraryExpression: Arbitrary[Expression] = Arbitrary(
    Gen.lzy(expressionGenerator)
  )
}

object ExpressionArbitraries extends ExpressionArbitraries
