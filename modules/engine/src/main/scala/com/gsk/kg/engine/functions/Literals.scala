package com.gsk.kg.engine.functions

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{concat => cc, _}

object Literals {

  // scalastyle:off
  val nullLiteral = lit(null)
  // scalastyle:on

  sealed trait Literal {
    def value: Column
    def tag: Column
  }

  final case class LocalizedLiteral(value: Column, tag: Column) extends Literal
  final case class TypedLiteral(value: Column, tag: Column)     extends Literal
  final case class NumericLiteral(value: Column, tag: Column)   extends Literal
  object NumericLiteral {
    def apply(col: Column): NumericLiteral = {
      new NumericLiteral(
        regexp_replace(substring_index(col, "^^", 1), "\"", ""),
        substring_index(col, "^^", -1)
      )
    }
  }

  def isNumericLiteral(col: Column): Column =
    isIntNumericLiteral(col) ||
      isDecimalNumericLiteral(col) ||
      isFloatNumericLiteral(col) ||
      isDoubleNumericLiteral(col)

  def isIntNumericLiteral(col: Column): Column = {
    val typed = TypedLiteral(col)
    typed.tag === lit("xsd:int") ||
    typed.tag === lit("<http://www.w3.org/2001/XMLSchema#int>") ||
    typed.tag === lit("xsd:integer") ||
    typed.tag === lit("<http://www.w3.org/2001/XMLSchema#integer>") ||
    (typed.value.cast("int").isNotNull && !typed.value.contains("."))
  }

  def isDecimalNumericLiteral(col: Column): Column = {
    val typed = TypedLiteral(col)
    typed.tag === lit("xsd:decimal") ||
    typed.tag === lit("<http://www.w3.org/2001/XMLSchema#decimal>") ||
    (typed.value.cast("decimal").isNotNull && !typed.value.contains("."))
  }

  def isFloatNumericLiteral(col: Column): Column = {
    val typed = TypedLiteral(col)
    typed.tag === lit("xsd:float") ||
    typed.tag === lit("<http://www.w3.org/2001/XMLSchema#float>") ||
    (typed.value.cast("float").isNotNull && typed.value.contains("."))
  }

  def isDoubleNumericLiteral(col: Column): Column = {
    val typed = TypedLiteral(col)
    typed.tag === lit("xsd:double") ||
    typed.tag === lit("<http://www.w3.org/2001/XMLSchema#double>") ||
    typed.tag === lit("xsd:numeric") ||
    typed.tag === lit("<http://www.w3.org/2001/XMLSchema#numeric>") ||
    (typed.value.cast("double").isNotNull && typed.value.contains("."))
  }

  def applyNotPromote(col1: Column, col2: Column)(
      op: (Column, Column) => Column
  ): Column = {
    val l = NumericLiteral(col1)
    val r = NumericLiteral(col2)
    op(l.value, r.value)
  }

  def applyPromoteLeftInt(col1: Column, col2: Column)(
      op: (Column, Column) => Column
  ): Column = {
    val l = NumericLiteral(col1)
    val r = NumericLiteral(col2)
    op(l.value.cast("int"), r.value)
  }

  def applyPromoteLeftDecimal(col1: Column, col2: Column)(
      op: (Column, Column) => Column
  ): Column = {
    val l = NumericLiteral(col1)
    val r = NumericLiteral(col2)
    op(l.value.cast("decimal"), r.value)
  }

  def applyPromoteLeftFloat(col1: Column, col2: Column)(
      op: (Column, Column) => Column
  ): Column = {
    val l = NumericLiteral(col1)
    val r = NumericLiteral(col2)
    op(l.value.cast("float"), r.value)
  }

  def applyPromoteLeftDouble(col1: Column, col2: Column)(
      op: (Column, Column) => Column
  ): Column = {
    val l = NumericLiteral(col1)
    val r = NumericLiteral(col2)
    op(l.value.cast("double"), r.value)
  }

  def applyPromoteRightInt(col1: Column, col2: Column)(
      op: (Column, Column) => Column
  ): Column = {
    val l = NumericLiteral(col1)
    val r = NumericLiteral(col2)
    op(l.value.cast("int"), r.value)
  }

  def applyPromoteRightDecimal(col1: Column, col2: Column)(
      op: (Column, Column) => Column
  ): Column = {
    val l = NumericLiteral(col1)
    val r = NumericLiteral(col2)
    op(l.value.cast("decimal"), r.value)
  }

  def applyPromoteRightFloat(col1: Column, col2: Column)(
      op: (Column, Column) => Column
  ): Column = {
    val l = NumericLiteral(col1)
    val r = NumericLiteral(col2)
    op(l.value.cast("float"), r.value)
  }

  def applyPromoteRightDouble(col1: Column, col2: Column)(
      op: (Column, Column) => Column
  ): Column = {
    val l = NumericLiteral(col1)
    val r = NumericLiteral(col2)
    op(l.value.cast("double"), r.value)
  }

  // scalastyle:off
  def promoteNumericBoolean(col1: Column, col2: Column)(
      op: (Column, Column) => Column
  ): Column = {
    when( // Int, Int -> Int
      isIntNumericLiteral(col1) && isIntNumericLiteral(col2),
      applyNotPromote(col1, col2)(op)
    ).when( // Int, Decimal -> Decimal
      isIntNumericLiteral(col1) && isDecimalNumericLiteral(col2),
      applyPromoteLeftDecimal(col1, col2)(op)
    ).when( // Int, Float -> Float
      isIntNumericLiteral(col1) && isFloatNumericLiteral(col2),
      applyPromoteLeftFloat(col1, col2)(op)
    ).when( // Int, Double -> Double
      isIntNumericLiteral(col1) && isDoubleNumericLiteral(col2),
      applyPromoteLeftDouble(col1, col2)(op)
    ).when( // Decimal, Int -> Decimal
      isDecimalNumericLiteral(col1) && isIntNumericLiteral(col2),
      applyPromoteRightDecimal(col1, col2)(op)
    ).when( // Decimal, Decimal -> Decimal
      isDecimalNumericLiteral(col1) && isDecimalNumericLiteral(col2),
      applyNotPromote(col1, col2)(op)
    ).when( // Decimal, Float -> Float
      isDecimalNumericLiteral(col1) && isFloatNumericLiteral(col2),
      applyPromoteLeftFloat(col1, col2)(op)
    ).when( // Decimal, Double -> Double
      isDecimalNumericLiteral(col1) && isDoubleNumericLiteral(col2),
      applyPromoteLeftDouble(col1, col2)(op)
    ).when( // Float, Int -> Float
      isFloatNumericLiteral(col1) && isIntNumericLiteral(col2),
      applyPromoteRightFloat(col1, col2)(op)
    ).when( // Float, Decimal -> Float
      isFloatNumericLiteral(col1) && isDecimalNumericLiteral(col2),
      applyPromoteRightFloat(col1, col2)(op)
    ).when( // Float, Float -> Float
      isFloatNumericLiteral(col1) && isFloatNumericLiteral(col2),
      applyNotPromote(col1, col2)(op)
    ).when( // Float, Double -> Double
      isFloatNumericLiteral(col1) && isDoubleNumericLiteral(col2),
      applyPromoteLeftDouble(col1, col2)(op)
    ).when( // Double, Int -> Double
      isDoubleNumericLiteral(col1) && isIntNumericLiteral(col2),
      applyPromoteRightDouble(col1, col2)(op)
    ).when( // Double, Decimal -> Double
      isDoubleNumericLiteral(col1) && isDecimalNumericLiteral(col2),
      applyPromoteRightDouble(col1, col2)(op)
    ).when( // Double, Float -> Double
      isDoubleNumericLiteral(col1) && isFloatNumericLiteral(col2),
      applyPromoteRightDouble(col1, col2)(op)
    ).when( // Double, Double -> Double
      isDoubleNumericLiteral(col1) && isDoubleNumericLiteral(col2),
      applyNotPromote(col1, col2)(op)
    )
  }
  // scalastyle:on

  object LocalizedLiteral {
    def apply(c: Column): LocalizedLiteral = {
      new LocalizedLiteral(
        substring_index(c, "@", 1),
        substring_index(c, "@", -1)
      )
    }

    def apply(s: String): LocalizedLiteral = {
      val split = s.split("@").toSeq
      new LocalizedLiteral(
        lit(split.head.replace("\"", "")),
        lit(split.last)
      )
    }

    def formatLocalized(
        l: LocalizedLiteral,
        s: String,
        localizedFormat: String
    )(
        f: (Column, String) => Column
    ): Column =
      when(
        f(l.value, s) === lit(""),
        f(l.value, s)
      ).otherwise(
        cc(
          format_string(localizedFormat, f(l.value, s)),
          l.tag
        )
      )
  }

  object TypedLiteral {
    def apply(c: Column): TypedLiteral = {
      new TypedLiteral(
        substring_index(c, "^^", 1),
        substring_index(c, "^^", -1)
      )
    }

    def apply(s: String): TypedLiteral = {
      val split = s.split("\\^\\^")
      new TypedLiteral(
        lit(split.head.replace("\"", "")),
        lit(split.last)
      )
    }

    def formatTyped(t: TypedLiteral, s: String, typedFormat: String)(
        f: (Column, String) => Column
    ): Column = when(
      f(t.value, s) === lit(""),
      f(t.value, s)
    ).otherwise(
      cc(
        format_string(typedFormat, f(t.value, s)),
        t.tag
      )
    )
  }
}
