package com.gsk.kg.engine

import com.gsk.kg.sparqlparser.Expr
import com.gsk.kg.sparqlparser.StringVal.VARIABLE

sealed trait Predicate

object Predicate {
  case class SPO(s: String, p: String, o: String) extends Predicate
  case class SP(s: String, p: String)             extends Predicate
  case class PO(p: String, o: String)             extends Predicate
  case class SO(s: String, o: String)             extends Predicate
  case class S(s: String)                         extends Predicate
  case class P(p: String)                         extends Predicate
  case class O(o: String)                         extends Predicate
  case object None                                extends Predicate

  def fromQuad(Quad: Expr.Quad): Predicate =
    Quad match {
      case Expr.Quad(VARIABLE(_), VARIABLE(_), VARIABLE(_), _) => Predicate.None
      case Expr.Quad(s, VARIABLE(_), VARIABLE(_), _)           => Predicate.S(s.s)
      case Expr.Quad(VARIABLE(_), p, VARIABLE(_), _)           => Predicate.P(p.s)
      case Expr.Quad(VARIABLE(_), VARIABLE(_), o, _)           => Predicate.O(o.s)
      case Expr.Quad(s, p, VARIABLE(_), _)                     => Predicate.SP(s.s, p.s)
      case Expr.Quad(VARIABLE(_), p, o, _)                     => Predicate.PO(p.s, o.s)
      case Expr.Quad(s, VARIABLE(_), o, _)                     => Predicate.SO(s.s, o.s)
      case Expr.Quad(s, p, o, _)                               => Predicate.SPO(s.s, p.s, o.s)
    }
}
