package com.gsk.kg.engine.functions

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{concat => cc, _}
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.StringType

import com.gsk.kg.engine.RdfFormatter

object Literals {

  // scalastyle:off
  val nullLiteral: Column    = lit(null)
  val TrueCol: Column        = lit(true)
  val FalseCol: Column       = lit(false)
  val EmptyStringCol: Column = lit("")
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
        trim(substring_index(col, "^^", 1), "\""),
        substring_index(col, "^^", -1)
      )
    }

    object BooleanResult {

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
    }

    object NumericResult {

      def applyNotPromote(col1: Column, col2: Column)(
          op: (Column, Column) => Column
      ): Column = {
        val l        = NumericLiteral(col1)
        val r        = NumericLiteral(col2)
        val opResult = op(l.value, r.value)

        when(
          isPlainLiteral(col1) && isPlainLiteral(col2),
          opResult
        ).when(
          (isIntNumericLiteral(col1) && isIntNumericLiteral(col1)) ||
            (isDecimalNumericLiteral(col1) || isDecimalNumericLiteral(col2)),
          format_string("\"%s\"^^%s", format_number(opResult, 0), r.tag)
        ).otherwise(
          format_string("\"%s\"^^%s", opResult, r.tag)
        )
      }

      def applyPromoteLeftInt(col1: Column, col2: Column)(
          op: (Column, Column) => Column
      ): Column = {
        val l = NumericLiteral(col1)
        val r = NumericLiteral(col2)
        val opResult =
          format_number(op(l.value.cast("int"), r.value.cast("int")), 0)
        format_string("\"%s\"^^%s", opResult, r.tag)
      }

      def applyPromoteLeftDecimal(col1: Column, col2: Column)(
          op: (Column, Column) => Column
      ): Column = {
        val l = NumericLiteral(col1)
        val r = NumericLiteral(col2)
        val opResult =
          format_number(op(l.value.cast("decimal"), r.value.cast("decimal")), 0)
        format_string("\"%s\"^^%s", opResult, r.tag)
      }

      def applyPromoteLeftFloat(col1: Column, col2: Column)(
          op: (Column, Column) => Column
      ): Column = {
        val l        = NumericLiteral(col1)
        val r        = NumericLiteral(col2)
        val opResult = op(l.value.cast("float"), r.value.cast("float"))
        format_string("\"%s\"^^%s", opResult, r.tag)
      }

      def applyPromoteLeftDouble(col1: Column, col2: Column)(
          op: (Column, Column) => Column
      ): Column = {
        val l        = NumericLiteral(col1)
        val r        = NumericLiteral(col2)
        val opResult = op(l.value.cast("double"), r.value.cast("double"))
        format_string("\"%s\"^^%s", opResult, r.tag)
      }

      def applyPromoteRightInt(col1: Column, col2: Column)(
          op: (Column, Column) => Column
      ): Column = {
        val l = NumericLiteral(col1)
        val r = NumericLiteral(col2)
        val opResult =
          format_number(op(l.value.cast("int"), r.value.cast("int")), 0)
        format_string("\"%s\"^^%s", opResult, l.tag)
      }

      def applyPromoteRightDecimal(col1: Column, col2: Column)(
          op: (Column, Column) => Column
      ): Column = {
        val l = NumericLiteral(col1)
        val r = NumericLiteral(col2)
        val opResult =
          format_number(op(l.value.cast("decimal"), r.value.cast("decimal")), 0)
        format_string("\"%s\"^^%s", opResult, l.tag)
      }

      def applyPromoteRightFloat(col1: Column, col2: Column)(
          op: (Column, Column) => Column
      ): Column = {
        val l        = NumericLiteral(col1)
        val r        = NumericLiteral(col2)
        val opResult = op(l.value.cast("float"), r.value.cast("float"))
        format_string("\"%s\"^^%s", opResult, l.tag)
      }

      def applyPromoteRightDouble(col1: Column, col2: Column)(
          op: (Column, Column) => Column
      ): Column = {
        val l        = NumericLiteral(col1)
        val r        = NumericLiteral(col2)
        val opResult = op(l.value.cast("double"), r.value.cast("double"))
        format_string("\"%s\"^^%s", opResult, l.tag)
      }
    }
  }

  object StringLiteral {

    def applyPromoteRightTyped(col1: Column, col2: Column)(
        op: (Column, Column) => Column
    ): Column = {
      val l = TypedLiteral(col1)
      op(l.value, trim(col2, "\""))
    }

    def applyPromoteLeftTyped(col1: Column, col2: Column)(
        op: (Column, Column) => Column
    ): Column = {
      val r = TypedLiteral(col2)
      op(trim(col1, "\""), r.value)
    }

    def applyNotPromoteTyped(col1: Column, col2: Column)(
        op: (Column, Column) => Column
    ): Column =
      op(col1, col2)

    def applyNotPromotePlain(col1: Column, col2: Column)(
        op: (Column, Column) => Column
    ): Column =
      op(trim(col1, "\""), trim(col2, "\""))
  }

  object BooleanLiteral {

    def applyPromoteRightTyped(col1: Column, col2: Column)(
        op: (Column, Column) => Column
    ): Column = {
      val l = TypedLiteral(col1)
      op(l.value, col2)
    }

    def applyPromoteLeftTyped(col1: Column, col2: Column)(
        op: (Column, Column) => Column
    ): Column = {
      val r = TypedLiteral(col2)
      op(col1, r.value)
    }

    def applyNotPromote(col1: Column, col2: Column)(
        op: (Column, Column) => Column
    ): Column =
      op(col1, col2)
  }

  def isPlainLiteral(col: Column): Column = {
    val typed = TypedLiteral(col)
    typed.tag === lit("")
  }

  def isPlainNumericFloatingPoint(col: Column): Column = {
    isPlainLiteral(col) && {
      val typed = TypedLiteral(col)
      typed.value.cast("double").isNotNull && typed.value.contains(".")
    }
  }

  def isPlainNumericNotFloatingPoint(col: Column): Column = {
    isPlainLiteral(col) && {
      val typed = TypedLiteral(col)
      typed.value.cast("int").isNotNull && !typed.value.contains(".")
    }
  }

  def isPlainBoolean(col: Column): Column =
    isPlainLiteral(col) && {
      val typed = TypedLiteral(col)
      typed.value.cast(BooleanType).isNotNull && !typed.value.contains(".")
    }

  /** This function is used to infer schema of data according to jena executions:
    * @param col
    * @return
    * "value"   -> string
    * "3"       -> integer
    * "3.0"     -> decimal, be careful because "3.0" is decimal, not double
    * "\"3.0\"" -> string
    */
  def inferType(col: Column): Column = {
    when(
      RdfFormatter.isQuoted(col),
      format_string(
        "\"%s\"^^%s",
        extractStringLiteral(col),
        lit("xsd:string")
      )
    )
      .when(
        isPlainNumericNotFloatingPoint(col),
        format_string("\"%s\"^^%s", col, lit("xsd:integer"))
      )
      .when(
        isPlainNumericFloatingPoint(col),
        format_string("\"%s\"^^%s", col, lit("xsd:decimal"))
      )
      .when(
        isPlainBoolean(col),
        format_string("\"%s\"^^%s", col, lit("xsd:boolean"))
      )
      .otherwise(format_string("\"%s\"^^%s", col, lit("xsd:string")))
  }

  def isStringLiteral(col: Column): Column = {
    val typed = TypedLiteral(col)
    typed.tag === lit("xsd:string") ||
    typed.tag === lit("<http://www.w3.org/2001/XMLSchema#string>")
  }

  def isBooleanLiteral(col: Column): Column = {
    val typed = TypedLiteral(col)
    typed.tag === lit("xsd:boolean") ||
    typed.tag === lit("<http://www.w3.org/2001/XMLSchema#boolean>")
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
    typed.tag === lit("<http://www.w3.org/2001/XMLSchema#integer>")
  }

  def isDecimalNumericLiteral(col: Column): Column = {
    val typed = TypedLiteral(col)
    typed.tag === lit("xsd:decimal") ||
    typed.tag === lit("<http://www.w3.org/2001/XMLSchema#decimal>")
  }

  def isFloatNumericLiteral(col: Column): Column = {
    val typed = TypedLiteral(col)
    typed.tag === lit("xsd:float") ||
    typed.tag === lit("<http://www.w3.org/2001/XMLSchema#float>")
  }

  def isDoubleNumericLiteral(col: Column): Column = {
    val typed = TypedLiteral(col)
    typed.tag === lit("xsd:double") ||
    typed.tag === lit("<http://www.w3.org/2001/XMLSchema#double>") ||
    typed.tag === lit("xsd:numeric") ||
    typed.tag === lit("<http://www.w3.org/2001/XMLSchema#numeric>")
  }

  def isDateTimeLiteral(col: Column): Column = {
    val typed = TypedLiteral(col)
    typed.tag === lit("xsd:dateTime") ||
    typed.tag === lit("<http://www.w3.org/2001/XMLSchema#dateTime>")
  }

  def promoteBooleanBooleanToBooleanResult(col1: Column, col2: Column)(
      op: (Column, Column) => Column
  ): Column = {
    import BooleanLiteral._

    when(
      isPlainLiteral(col1) && isPlainLiteral(col2),
      applyNotPromote(col1, col2)(op)
    ).when(
      isPlainLiteral(col1) && isBooleanLiteral(col2),
      applyPromoteLeftTyped(col1, col2)(op)
    ).when(
      isBooleanLiteral(col1) && isPlainLiteral(col2),
      applyPromoteRightTyped(col1, col2)(op)
    ).when(
      isBooleanLiteral(col1) && isBooleanLiteral(col2),
      applyNotPromote(col1, col2)(op)
    )
  }

  def promoteStringArgsToBooleanResult(col1: Column, col2: Column)(
      op: (Column, Column) => Column
  ): Column = {
    import StringLiteral._

    when(
      isPlainLiteral(col1) && isPlainLiteral(col2),
      applyNotPromotePlain(col1, col2)(op)
    ).when(
      isPlainLiteral(col1) && isStringLiteral(col2),
      applyPromoteLeftTyped(col1, col2)(op)
    ).when(
      isStringLiteral(col1) && isPlainLiteral(col2),
      applyPromoteRightTyped(col1, col2)(op)
    ).when(
      isStringLiteral(col1) && isStringLiteral(col2),
      applyNotPromoteTyped(col1, col2)(op)
    )
  }

  // scalastyle:off
  def promoteNumericArgsToNumericResult(col1: Column, col2: Column)(
      op: (Column, Column) => Column
  ): Column = {
    import NumericLiteral.NumericResult._

    when( // Plain, Plain => Plain
      isPlainLiteral(col1) && isPlainLiteral(col2),
      applyNotPromote(col1, col2)(op)
    ).when( // Plain, Int => Int
      isPlainLiteral(col1) && isIntNumericLiteral(col2),
      applyPromoteLeftInt(col1, col2)(op)
    ).when( // Plain, Decimal => Decimal
      isPlainLiteral(col1) && isDecimalNumericLiteral(col2),
      applyPromoteLeftDecimal(col1, col2)(op)
    ).when( // Plain, Float => Float
      isPlainLiteral(col1) && isFloatNumericLiteral(col2),
      applyPromoteLeftFloat(col1, col2)(op)
    ).when( // Plain, Double => Double
      isPlainLiteral(col1) && isDoubleNumericLiteral(col2),
      applyPromoteLeftDouble(col1, col2)(op)
    ).when( // Int, Plain => Int
      isIntNumericLiteral(col1) && isPlainLiteral(col2),
      applyPromoteRightInt(col1, col2)(op)
    ).when( // Int, Int => Int
      isIntNumericLiteral(col1) && isIntNumericLiteral(col2),
      applyNotPromote(col1, col2)(op)
    ).when( // Int, Decimal => Decimal
      isIntNumericLiteral(col1) && isDecimalNumericLiteral(col2),
      applyPromoteLeftDecimal(col1, col2)(op)
    ).when( // Int, Float => Float
      isIntNumericLiteral(col1) && isFloatNumericLiteral(col2),
      applyPromoteLeftFloat(col1, col2)(op)
    ).when( // Int, Double => Double
      isIntNumericLiteral(col1) && isDoubleNumericLiteral(col2),
      applyPromoteLeftDouble(col1, col2)(op)
    ).when( // Decimal, Plain => Decimal
      isDecimalNumericLiteral(col1) && isPlainLiteral(col2),
      applyPromoteRightDecimal(col1, col2)(op)
    ).when( // Decimal, Int => Decimal
      isDecimalNumericLiteral(col1) && isIntNumericLiteral(col2),
      applyPromoteRightDecimal(col1, col2)(op)
    ).when( // Decimal, Decimal => Decimal
      isDecimalNumericLiteral(col1) && isDecimalNumericLiteral(col2),
      applyNotPromote(col1, col2)(op)
    ).when( // Decimal, Float => Float
      isDecimalNumericLiteral(col1) && isFloatNumericLiteral(col2),
      applyPromoteLeftFloat(col1, col2)(op)
    ).when( // Decimal, Double => Double
      isDecimalNumericLiteral(col1) && isDoubleNumericLiteral(col2),
      applyPromoteLeftDouble(col1, col2)(op)
    ).when( // Float, Plain => Float
      isFloatNumericLiteral(col1) && isPlainLiteral(col2),
      applyPromoteRightFloat(col1, col2)(op)
    ).when( // Float, Int => Float
      isFloatNumericLiteral(col1) && isIntNumericLiteral(col2),
      applyPromoteRightFloat(col1, col2)(op)
    ).when( // Float, Decimal => Float
      isFloatNumericLiteral(col1) && isDecimalNumericLiteral(col2),
      applyPromoteRightFloat(col1, col2)(op)
    ).when( // Float, Float => Float
      isFloatNumericLiteral(col1) && isFloatNumericLiteral(col2),
      applyNotPromote(col1, col2)(op)
    ).when( // Float, Double => Double
      isFloatNumericLiteral(col1) && isDoubleNumericLiteral(col2),
      applyPromoteLeftDouble(col1, col2)(op)
    ).when( // Double, Plain => Double
      isDoubleNumericLiteral(col1) && isPlainLiteral(col2),
      applyPromoteRightDouble(col1, col2)(op)
    ).when( // Double, Int => Double
      isDoubleNumericLiteral(col1) && isIntNumericLiteral(col2),
      applyPromoteRightDouble(col1, col2)(op)
    ).when( // Double, Decimal => Double
      isDoubleNumericLiteral(col1) && isDecimalNumericLiteral(col2),
      applyPromoteRightDouble(col1, col2)(op)
    ).when( // Double, Float => Double
      isDoubleNumericLiteral(col1) && isFloatNumericLiteral(col2),
      applyPromoteRightDouble(col1, col2)(op)
    ).when( // Double, Double => Double
      isDoubleNumericLiteral(col1) && isDoubleNumericLiteral(col2),
      applyNotPromote(col1, col2)(op)
    )
  }
  // scalastyle:on

  // scalastyle:off
  def promoteNumericArgsToBooleanResult(col1: Column, col2: Column)(
      op: (Column, Column) => Column
  ): Column = {
    import NumericLiteral.BooleanResult._

    when(
      isPlainNumericFloatingPoint(col1) && isPlainNumericFloatingPoint(col2),
      applyNotPromote(col1, col2)(op)
    ).when(
      isPlainNumericFloatingPoint(col1) && isPlainNumericNotFloatingPoint(col2),
      applyPromoteRightDouble(col1, col2)(op)
    ).when(
      isPlainNumericNotFloatingPoint(col2) && isPlainNumericFloatingPoint(col2),
      applyPromoteLeftDouble(col1, col2)(op)
    ).when(
      isPlainNumericFloatingPoint(col1) && !isPlainLiteral(col2),
      applyPromoteLeftDouble(col1, col2)(op)
    ).when(
      isPlainNumericNotFloatingPoint(col1) && !isPlainLiteral(col2),
      applyPromoteRightInt(col1, col2)(op)
    ).when(
      !isPlainLiteral(col1) && isPlainNumericFloatingPoint(col2),
      applyPromoteLeftDouble(col1, col2)(op)
    ).when(
      !isPlainLiteral(col1) && isPlainNumericNotFloatingPoint(col2),
      applyPromoteLeftInt(col1, col2)(op)
    ).when( // Int, Int -> Int
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
      val value = trim(substring_index(c, "@", 1), "\"")
      val tag   = substring_index(c, "@", -1)
      new LocalizedLiteral(
        value,
        when(
          value === tag,
          lit("")
        ).otherwise(tag)
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
      val value = trim(substring_index(c, "^^", 1), "\"")
      val tag   = trim(substring_index(c, "^^", -1), "\"")
      new TypedLiteral(
        value,
        when(
          value === tag,
          lit("")
        ).otherwise(
          tag
        )
      )
    }

    def apply(s: String): TypedLiteral = {
      val split = s.split("\\^\\^")
      new TypedLiteral(
        lit(split.head.replace("\"", "")),
        lit(split.last)
      )
    }

    def isTypedLiteral(col: Column): Column =
      col.startsWith("\"") && col.contains("\"^^")

    def formatTyped(t: TypedLiteral, s: String, typedFormat: String)(
        f: (Column, String) => Column
    ): Column = when(
      f(t.value, s) === EmptyStringCol,
      f(t.value, s)
    ).otherwise(
      cc(
        format_string(typedFormat, f(t.value, s)),
        t.tag
      )
    )
  }

  object DateLiteral {

    /** This helper method tries to parse a datetime expressed as a RDF
      * datetime string `"0193-07-03T20:50:09.000+04:00"^^xsd:dateTime`
      * to a column with underlying type datetime.
      *
      * @param col
      * @return
      */
    def parseDateFromRDFDateTime(col: Column): Column =
      when(
        regexp_extract(col, ExtractDateTime, 1) =!= EmptyStringCol,
        to_timestamp(regexp_extract(col, ExtractDateTime, 1))
      ).otherwise(nullLiteral)

    def applyDateTimeLiteral(l: Column, r: Column)(
        operator: (Column, Column) => Column
    ): Column =
      when(
        regexp_extract(
          l.cast(StringType),
          ExtractDateTime,
          1
        ) =!= EmptyStringCol &&
          regexp_extract(
            r.cast(StringType),
            ExtractDateTime,
            1
          ) =!= EmptyStringCol,
        operator(
          parseDateFromRDFDateTime(l.cast(StringType)),
          parseDateFromRDFDateTime(r.cast(StringType))
        )
      )

    private val ExtractDateTime = """^"(.*)"\^\^(.*)dateTime(.*)$"""
  }

  def extractStringLiteral(str: String): String =
    str match {
      case s if str.contains("\"@") || str.contains("\"^^") =>
        s.stripPrefix("\"").split("\"").head
      case s => s.stripPrefix("\"").stripSuffix("\"")
    }

  def extractStringLiteral(col: Column): Column =
    when(
      col.startsWith("\"") && col.contains("\"@"),
      trim(substring_index(col, "\"@", 1), "\"")
    )
      .when(
        col.startsWith("\"") && col.contains("\"^^"),
        trim(substring_index(col, "\"^^", 1), "\"")
      )
      .otherwise(trim(col, "\""))
}
