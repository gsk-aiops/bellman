package com.gsk.kg.sparqlparser

/**
 * @see Model after [[https://www.w3.org/TR/sparql11-query/#rExpression]]
 */
sealed trait Expression

sealed trait Conditional extends Expression

object Conditional {
  final case class EQUALS(l:Expression, r:Expression) extends Conditional
  final case class NOTEQUALS(l: Expression, r:Expression) extends Conditional
  final case class GT(l:Expression, r:Expression) extends Conditional
  final case class GTE(l: Expression, r:Expression) extends Conditional
  final case class LT(l:Expression, r:Expression) extends Conditional
  final case class LTE(l:Expression, r: Expression) extends Conditional
  final case class OR(l:Expression, r:Expression) extends Conditional
  final case class AND(l:Expression, r:Expression) extends Conditional
  final case class NEGATE(s: Expression) extends Conditional
}

sealed trait StringLike extends Expression

sealed trait BuildInFunc extends StringLike

sealed trait StringVal extends StringLike {
  val s: String
  def isVariable: Boolean = this match {
    case StringVal.STRING(s,_) => false
    case StringVal.NUM(s) => false
    case StringVal.VARIABLE(s) => true
    case StringVal.URIVAL(s) => false
    case StringVal.BLANK(s) => false
    case StringVal.BOOL(_) => false
  }
  def isBlank: Boolean = this match {
    case StringVal.STRING(s,_) => false
    case StringVal.NUM(s) => false
    case StringVal.VARIABLE(s) => false
    case StringVal.URIVAL(s) => false
    case StringVal.BLANK(s) => true
    case StringVal.BOOL(_) => false
  }
}

object BuildInFunc {
  final case class URI(s:Expression) extends BuildInFunc
  final case class CONCAT(appendTo:Expression, append:Expression) extends BuildInFunc
  final case class STR(s:Expression) extends BuildInFunc
  final case class STRAFTER(s:Expression, f:Expression) extends BuildInFunc
  final case class STRSTARTS(l:Expression, r:Expression) extends BuildInFunc
  final case class ISBLANK(s: Expression) extends BuildInFunc
  final case class REPLACE(st: Expression, pattern: Expression, by: Expression) extends BuildInFunc
  final case class REGEX(l:Expression, r:Expression) extends BuildInFunc
}

object StringVal {
  final case class STRING(s:String, tag: Option[String] = None) extends StringVal
  final case class NUM(s:String) extends StringVal
  final case class VARIABLE(s:String) extends StringVal
  final case class URIVAL(s:String) extends StringVal
  final case class BLANK(s:String) extends StringVal
  final case class BOOL(s:String) extends StringVal
}
