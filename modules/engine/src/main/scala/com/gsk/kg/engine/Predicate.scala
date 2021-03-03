package com.gsk.kg.engine

import com.gsk.kg.sparqlparser.Expr
import com.gsk.kg.sparqlparser.StringVal.VARIABLE
import com.gsk.kg.sparqlparser.StringVal
import com.gsk.kg.sparqlparser.StringVal.STRING
import com.gsk.kg.sparqlparser.StringVal.NUM
import com.gsk.kg.sparqlparser.StringVal.URIVAL
import com.gsk.kg.sparqlparser.StringVal.BLANK
import com.gsk.kg.sparqlparser.StringVal.BOOL

sealed trait Predicate

object Predicate {
  case class SPO(s: String, p: String, o: String) extends Predicate
  case class SP(s: String, p: String) extends Predicate
  case class PO(p: String, o: String) extends Predicate
  case class SO(s: String, o: String) extends Predicate
  case class S(s: String) extends Predicate
  case class P(p: String) extends Predicate
  case class O(o: String) extends Predicate
  case object None extends Predicate


  def fromTriple(triple: Expr.Triple): Predicate =
    triple match {
      case Expr.Triple(VARIABLE(_), VARIABLE(_), VARIABLE(_)) => Predicate.None
      case Expr.Triple(s, VARIABLE(_), VARIABLE(_))           => Predicate.S(getLiteral(s))
      case Expr.Triple(VARIABLE(_), p, VARIABLE(_))           => Predicate.P(getLiteral(p))
      case Expr.Triple(VARIABLE(_), VARIABLE(_), o)           => Predicate.O(getLiteral(o))
      case Expr.Triple(s, p, VARIABLE(_))                     => Predicate.SP(getLiteral(s), getLiteral(p))
      case Expr.Triple(VARIABLE(_), p, o)                     => Predicate.PO(getLiteral(p), getLiteral(o))
      case Expr.Triple(s, VARIABLE(_), o)                     => Predicate.SO(getLiteral(s), getLiteral(o))
      case Expr.Triple(s, p, o)                               => Predicate.SPO(getLiteral(s), getLiteral(p), getLiteral(o))
    }

  def getLiteral(x: StringVal): String =
    x match {
	    case STRING(s, tag) => s"'$s'"
	    case NUM(s) => s
	    case VARIABLE(s) => s
	    case URIVAL(s) => s.stripPrefix("<").stripSuffix(">")
	    case BLANK(s) => s
	    case BOOL(s) => s
    }
}
