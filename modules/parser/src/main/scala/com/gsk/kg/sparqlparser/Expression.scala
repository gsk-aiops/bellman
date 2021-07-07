package com.gsk.kg.sparqlparser

import com.gsk.kg.sparqlparser.StringVal.URIVAL

/** @see Model after [[https://www.w3.org/TR/sparql11-query/#rExpression]]
  */
sealed trait Expression

sealed trait Arithmetic extends Expression

object Arithmetic {
  final case class ADD(l: Expression, r: Expression)      extends Arithmetic
  final case class SUBTRACT(l: Expression, r: Expression) extends Arithmetic
  final case class MULTIPLY(l: Expression, r: Expression) extends Arithmetic
  final case class DIVIDE(l: Expression, r: Expression)   extends Arithmetic
}

sealed trait Conditional extends Expression

object Conditional {
  final case class EQUALS(l: Expression, r: Expression)    extends Conditional
  final case class GT(l: Expression, r: Expression)        extends Conditional
  final case class GTE(l: Expression, r: Expression)       extends Conditional
  final case class LT(l: Expression, r: Expression)        extends Conditional
  final case class LTE(l: Expression, r: Expression)       extends Conditional
  final case class OR(l: Expression, r: Expression)        extends Conditional
  final case class AND(l: Expression, r: Expression)       extends Conditional
  final case class NEGATE(s: Expression)                   extends Conditional
  final case class IN(e: Expression, xs: List[Expression]) extends Conditional
  final case class SAMETERM(l: Expression, r: Expression)  extends Conditional
  final case class IF(
      cnd: Expression,
      ifTrue: Expression,
      ifFalse: Expression
  )                                               extends Conditional
  final case class BOUND(e: Expression)           extends Conditional
  final case class COALESCE(xs: List[Expression]) extends Conditional
}

sealed trait StringLike extends Expression

sealed trait BuiltInFunc extends StringLike
object BuiltInFunc {
  final case class URI(s: Expression) extends BuiltInFunc
  final case class CONCAT(
      appendTo: Expression,
      append: List[Expression]
  )                                    extends BuiltInFunc
  final case class STR(s: Expression)  extends BuiltInFunc
  final case class LANG(s: Expression) extends BuiltInFunc
  final case class LANGMATCHES(s: Expression, range: Expression)
      extends BuiltInFunc
  final case class LCASE(s: Expression)                    extends BuiltInFunc
  final case class UCASE(s: Expression)                    extends BuiltInFunc
  final case class ISLITERAL(s: Expression)                extends BuiltInFunc
  final case class STRAFTER(s: Expression, f: Expression)  extends BuiltInFunc
  final case class STRBEFORE(s: Expression, f: Expression) extends BuiltInFunc
  final case class STRSTARTS(s: Expression, f: Expression) extends BuiltInFunc
  final case class STRENDS(s: Expression, f: Expression)   extends BuiltInFunc
  final case class STRDT(s: Expression, uri: URIVAL)       extends BuiltInFunc
  final case class STRLANG(s: Expression, l: Expression)   extends BuiltInFunc
  final case class STRLEN(s: Expression)                   extends BuiltInFunc
  final case class SUBSTR(
      s: Expression,
      pos: Expression,
      length: Option[Expression] = None
  )                                              extends BuiltInFunc
  final case class ISBLANK(s: Expression)        extends BuiltInFunc
  final case class ISNUMERIC(s: Expression)      extends BuiltInFunc
  final case class ENCODE_FOR_URI(s: Expression) extends BuiltInFunc
  final case class MD5(s: Expression)            extends BuiltInFunc
  final case class SHA1(s: Expression)           extends BuiltInFunc
  final case class SHA256(s: Expression)         extends BuiltInFunc
  final case class SHA384(s: Expression)         extends BuiltInFunc
  final case class SHA512(s: Expression)         extends BuiltInFunc
  final case class REPLACE(
      st: Expression,
      pattern: Expression,
      by: Expression,
      flags: Expression = StringVal.STRING("")
  ) extends BuiltInFunc
  final case class REGEX(
      s: Expression,
      pattern: Expression,
      flags: Expression = StringVal.STRING("")
  )                                     extends BuiltInFunc
  final case class UUID()               extends BuiltInFunc
  final case class CEIL(s: Expression)  extends BuiltInFunc
  final case class ROUND(s: Expression) extends BuiltInFunc
  final case class RAND()               extends BuiltInFunc
  final case class ABS(s: Expression)   extends BuiltInFunc
  final case class FLOOR(s: Expression) extends BuiltInFunc
  final case class STRUUID()            extends BuiltInFunc
}

sealed trait StringVal extends StringLike {
  val s: String
  def isVariable: Boolean = this match {
    case StringVal.STRING(s)           => false
    case StringVal.DT_STRING(s, tag)   => false
    case StringVal.LANG_STRING(s, tag) => false
    case StringVal.NUM(s)              => false
    case StringVal.VARIABLE(s)         => true
    case StringVal.GRAPH_VARIABLE      => true
    case StringVal.URIVAL(s)           => false
    case StringVal.BLANK(s)            => false
    case StringVal.BOOL(_)             => false
  }
  def isBlank: Boolean = this match {
    case StringVal.STRING(s)           => false
    case StringVal.DT_STRING(s, tag)   => false
    case StringVal.LANG_STRING(s, tag) => false
    case StringVal.NUM(s)              => false
    case StringVal.VARIABLE(s)         => false
    case StringVal.GRAPH_VARIABLE      => false
    case StringVal.URIVAL(s)           => false
    case StringVal.BLANK(s)            => true
    case StringVal.BOOL(_)             => false
  }
}
object StringVal {
  final case class STRING(s: String)                 extends StringVal
  final case class DT_STRING(s: String, tag: String) extends StringVal
  object DT_STRING {
    def toString(t: DT_STRING): String = s""""${t.s}"^^${t.tag}"""
  }
  final case class LANG_STRING(s: String, tag: String) extends StringVal
  object LANG_STRING {
    def toString(l: LANG_STRING): String = s""""${l.s}"@${l.tag}"""
  }
  final case class NUM(s: String)      extends StringVal
  final case class VARIABLE(s: String) extends StringVal
  final case object GRAPH_VARIABLE     extends StringVal { val s = "*g" }
  final case class URIVAL(s: String)   extends StringVal
  final case class BLANK(s: String)    extends StringVal
  final case class BOOL(s: String)     extends StringVal
}

sealed trait Aggregate extends Expression
object Aggregate {
  final case class COUNT(e: Expression)  extends Aggregate
  final case class SUM(e: Expression)    extends Aggregate
  final case class MIN(e: Expression)    extends Aggregate
  final case class MAX(e: Expression)    extends Aggregate
  final case class AVG(e: Expression)    extends Aggregate
  final case class SAMPLE(e: Expression) extends Aggregate
  final case class GROUP_CONCAT(e: Expression, separator: String)
      extends Aggregate
}

sealed trait ConditionOrder extends Expression
object ConditionOrder {
  final case class ASC(e: Expression)  extends ConditionOrder
  final case class DESC(e: Expression) extends ConditionOrder
}

sealed trait DateTimeFunc extends Expression
object DateTimeFunc {
  final case class NOW()                extends DateTimeFunc
  final case class YEAR(e: Expression)  extends DateTimeFunc
  final case class MONTH(e: Expression) extends DateTimeFunc
  final case class DAY(e: Expression)   extends DateTimeFunc
}
