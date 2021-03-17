package com.gsk.kg.sparqlparser

import com.gsk.kg.sparqlparser.StringVal.VARIABLE

import higherkindness.droste.macros.deriveFixedPoint

sealed trait Query {
  def r: Expr
}

object Query {
  import com.gsk.kg.sparqlparser.Expr.BGP
  final case class Describe(vars: Seq[VARIABLE], r: Expr,
                            defaultGraphs: List[StringVal.URIVAL] = List.empty,
                            namedGraphs: List[StringVal.URIVAL] = List.empty) extends Query
  final case class Ask(r: Expr,
                       defaultGraphs: List[StringVal.URIVAL] = List.empty,
                       namedGraphs: List[StringVal.URIVAL] = List.empty) extends Query
  final case class Construct(vars: Seq[VARIABLE],
                             bgp: BGP,
                             r: Expr,
                             defaultGraphs: List[StringVal.URIVAL] = List.empty,
                             namedGraphs: List[StringVal.URIVAL] = List.empty) extends Query
  final case class Select(vars: Seq[VARIABLE],
                          r: Expr,
                          defaultGraphs: List[StringVal.URIVAL] = List.empty,
                          namedGraphs: List[StringVal.URIVAL] = List.empty) extends Query
}

@deriveFixedPoint sealed trait Expr
object Expr {
  final case class BGP(triples:Seq[Triple]) extends Expr
  final case class Triple(s:StringVal, p:StringVal, o:StringVal) extends Expr {
    def getVariables: List[(StringVal, String)] = {
      List((s, "s"),(p, "p"),(o, "o")).filter(_._1.isVariable)
    }

    def getPredicates: List[(StringVal, String)] = {
      List((s, "s"),(p, "p"),(o, "o")).filter(!_._1.isVariable)
    }
  }
  final case class LeftJoin(l:Expr, r:Expr) extends Expr
  final case class FilteredLeftJoin(l:Expr, r:Expr, f:Seq[Expression]) extends Expr
  final case class Union(l:Expr, r:Expr) extends Expr
  final case class Extend(bindTo:VARIABLE, bindFrom:Expression, r:Expr) extends Expr
  final case class Filter(funcs:Seq[Expression], expr:Expr) extends Expr
  final case class Join(l:Expr, r:Expr) extends Expr
  final case class Graph(g:StringVal, e:Expr) extends Expr
  final case class Project(vars: Seq[VARIABLE], r:Expr) extends Expr
  final case class OffsetLimit(offset: Option[Long], limit: Option[Long], r:Expr) extends Expr
  final case class Distinct(r:Expr) extends Expr
  final case class OpNil() extends Expr
  final case class TabUnit() extends Expr
}
