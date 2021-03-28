package com.gsk.kg.sparql

import com.gsk.kg.sparqlparser.Expr
import com.gsk.kg.sparqlparser.Expr._
import com.gsk.kg.sparqlparser.Expression
import com.gsk.kg.sparqlparser.StringLike
import com.gsk.kg.sparqlparser.StringVal.VARIABLE

trait Visitor[T] {
  def visitQuad(quad: Quad): T

  def visitBGP(triples: Seq[T]): T

  def visitLeftJoin(left: T, right: T): T

  def visitFilteredLeftJoinVisitor(left: T, right: T, f: Seq[Expression]): T

  def visitUnion(left: T, right: T): T

  def visitExtend(to: VARIABLE, from: Expression, d: T): T

  def visitFilter(funcs: Seq[Expression], d: T): T

  def visitJoin(l: T, r: T): T

  def visitGraph(g: StringLike, d: T): T

  def visitSelect(vars: Seq[VARIABLE], d: T): T

  def visitOffsetLimit(off: Option[Long], lmt: Option[Long], d: T): T

  def visitDistinct(e: T): T

  def visitOpNil: T

  def visitTabUnit: T
}

object Visitors {

  def dispatch[T](expr: Expr, visitor: Visitor[T]): T = {
    expr match {
      case quad: Quad =>
        visitor.visitQuad(quad)
      case BGP(triples) =>
        val ts = triples.map(t => dispatch(t, visitor))
        visitor.visitBGP(ts)
      case LeftJoin(l, r) =>
        val left  = dispatch(l, visitor)
        val right = dispatch(r, visitor)
        visitor.visitLeftJoin(left, right)
      case FilteredLeftJoin(l, r, f) =>
        val left  = dispatch(l, visitor)
        val right = dispatch(r, visitor)
        visitor.visitFilteredLeftJoinVisitor(left, right, f)
      case Union(l, r) =>
        val left  = dispatch(l, visitor)
        val right = dispatch(r, visitor)
        visitor.visitUnion(left, right)
      case Extend(to, from, r) =>
        visitor.visitExtend(to, from, dispatch(r, visitor))
      case Filter(funcs, e) =>
        visitor.visitFilter(funcs, dispatch(e, visitor))
      case Join(l, r) =>
        val left  = dispatch(l, visitor)
        val right = dispatch(r, visitor)
        visitor.visitJoin(left, right)
      case Graph(g, e) =>
        visitor.visitGraph(g, dispatch(e, visitor))
      case Project(vars, r) =>
        visitor.visitSelect(vars, dispatch(r, visitor))
      case OffsetLimit(offset, limit, r) =>
        visitor.visitOffsetLimit(offset, limit, dispatch(r, visitor))
      case Distinct(r) =>
        visitor.visitDistinct(dispatch(r, visitor))
      case OpNil()   => visitor.visitOpNil
      case TabUnit() => visitor.visitTabUnit

    }
  }
}
