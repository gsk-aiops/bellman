package com.gsk.kg.sparqlparser

/** @see Model after [[https://www.w3.org/TR/sparql11-query/#rExpression]]
  */
sealed trait Expression

sealed trait Conditional extends Expression

object Conditional {
  final case class EQUALS(l: Expression, r: Expression)    extends Conditional
  final case class NOTEQUALS(l: Expression, r: Expression) extends Conditional
  final case class GT(l: Expression, r: Expression)        extends Conditional
  final case class GTE(l: Expression, r: Expression)       extends Conditional
  final case class LT(l: Expression, r: Expression)        extends Conditional
  final case class LTE(l: Expression, r: Expression)       extends Conditional
  final case class OR(l: Expression, r: Expression)        extends Conditional
  final case class AND(l: Expression, r: Expression)       extends Conditional
  final case class NEGATE(s: Expression)                   extends Conditional
}

sealed trait StringLike extends Expression

sealed trait BuiltInFunc extends StringLike
object BuiltInFunc {
  final case class URI(s: Expression) extends BuiltInFunc
  final case class CONCAT(appendTo: Expression, append: Expression)
      extends BuiltInFunc
  final case class STR(s: Expression)                      extends BuiltInFunc
  final case class STRAFTER(s: Expression, f: Expression)  extends BuiltInFunc
  final case class STRBEFORE(s: Expression, f: Expression) extends BuiltInFunc
  final case class STRSTARTS(s: Expression, f: Expression) extends BuiltInFunc
  final case class STRENDS(s: Expression, f: Expression)   extends BuiltInFunc
  final case class SUBSTR(
      s: Expression,
      pos: Expression,
      length: Option[Expression] = None
  )                                       extends BuiltInFunc
  final case class ISBLANK(s: Expression) extends BuiltInFunc
  final case class REPLACE(
      st: Expression,
      pattern: Expression,
      by: Expression,
      flags: Expression = StringVal.STRING("", None)
  ) extends BuiltInFunc
  final case class REGEX(
      s: Expression,
      pattern: Expression,
      flags: Expression = StringVal.STRING("", None)
  ) extends BuiltInFunc
}

sealed trait StringVal extends StringLike {
  val s: String
  def isVariable: Boolean = this match {
    case StringVal.STRING(s, _)   => false
    case StringVal.NUM(s)         => false
    case StringVal.VARIABLE(s)    => true
    case StringVal.GRAPH_VARIABLE => true
    case StringVal.URIVAL(s)      => false
    case StringVal.BLANK(s)       => false
    case StringVal.BOOL(_)        => false
  }
  def isBlank: Boolean = this match {
    case StringVal.STRING(s, _)   => false
    case StringVal.NUM(s)         => false
    case StringVal.VARIABLE(s)    => false
    case StringVal.GRAPH_VARIABLE => false
    case StringVal.URIVAL(s)      => false
    case StringVal.BLANK(s)       => true
    case StringVal.BOOL(_)        => false
  }
}
object StringVal {
  final case class STRING(s: String, tag: Option[String] = None)
      extends StringVal
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
