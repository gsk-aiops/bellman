package com.gsk.kg.sparqlparser

import higherkindness.droste.macros.deriveFixedPoint

import org.apache.jena.graph.Node
import org.apache.jena.sparql.core.{Quad => JenaQuad}

import com.gsk.kg.sparqlparser.StringVal.BLANK
import com.gsk.kg.sparqlparser.StringVal.GRAPH_VARIABLE
import com.gsk.kg.sparqlparser.StringVal.STRING
import com.gsk.kg.sparqlparser.StringVal.URIVAL
import com.gsk.kg.sparqlparser.StringVal.VARIABLE

sealed trait Query {
  def r: Expr
}

object Query {
  import com.gsk.kg.sparqlparser.Expr.BGP
  final case class Describe(
      vars: Seq[VARIABLE],
      r: Expr
  ) extends Query
  final case class Ask(
      r: Expr
  ) extends Query
  final case class Construct(
      vars: Seq[VARIABLE],
      bgp: BGP,
      r: Expr
  ) extends Query
  final case class Select(
      vars: Seq[VARIABLE],
      r: Expr
  ) extends Query
}

@deriveFixedPoint sealed trait Expr
object Expr {
  final case class BGP(quads: Seq[Quad]) extends Expr
  final case class Quad(
      s: StringVal,
      p: StringVal,
      o: StringVal,
      g: List[StringVal]
  ) extends Expr {
    def getVariables: List[(StringVal, String)] =
      getNamesAndPositions.filterNot(_._1 == GRAPH_VARIABLE)

    def getNamesAndPositions: List[(StringVal, String)] =
      List((s, "s"), (p, "p"), (o, "o")).filter(_._1.isVariable) ++
        g.collect { case e if e.isVariable => e -> "g" }

    def getPredicates: List[(StringVal, String)] =
      List((s, "s"), (p, "p"), (o, "o")).filter(!_._1.isVariable) ++
        g.collect { case e if !e.isVariable => e -> "g" }
  }
  object Quad {
    def apply(q: JenaQuad): Option[Quad] = {

      def toStringVal(n: Node): Option[StringVal] =
        if (n.isLiteral) {
          Some(STRING(n.toString))
        } else if (n == JenaQuad.defaultGraphNodeGenerated) {
          Some(GRAPH_VARIABLE)
        } else if (n.isURI) {
          Some(URIVAL("<" + n.toString + ">"))
        } else if (n.isVariable) {
          Some(VARIABLE(n.toString))
        } else if (n.isBlank) {
          Some(BLANK(n.toString))
        } else {
          None
        }

      (
        toStringVal(q.getSubject),
        toStringVal(q.getPredicate),
        toStringVal(q.getObject),
        toStringVal(q.getGraph)
      ) match {
        case (Some(s), Some(p), Some(o), Some(g)) =>
          Some(Quad(s, p, o, g :: Nil))
        case _ => None
      }
    }
    def toIter(q: JenaQuad): Iterable[Quad] = apply(q).toIterable
  }
  final case class LeftJoin(l: Expr, r: Expr) extends Expr
  final case class FilteredLeftJoin(l: Expr, r: Expr, f: Seq[Expression])
      extends Expr
  final case class Union(l: Expr, r: Expr) extends Expr
  final case class Extend(bindTo: VARIABLE, bindFrom: Expression, r: Expr)
      extends Expr
  final case class Filter(funcs: Seq[Expression], expr: Expr) extends Expr
  final case class Join(l: Expr, r: Expr)                     extends Expr
  final case class Graph(g: StringVal, e: Expr)               extends Expr
  final case class Project(vars: Seq[VARIABLE], r: Expr)      extends Expr
  final case class OffsetLimit(
      offset: Option[Long],
      limit: Option[Long],
      r: Expr
  ) extends Expr
  final case class Group(
      vars: Seq[VARIABLE],
      func: Seq[(VARIABLE, Expression)],
      r: Expr
  )                                                           extends Expr
  final case class Order(conds: Seq[ConditionOrder], r: Expr) extends Expr
  final case class Distinct(r: Expr)                          extends Expr
  final case class Table(vars: Seq[VARIABLE], rows: Seq[Row]) extends Expr
  final case class Row(tuples: Seq[(VARIABLE, StringVal)])    extends Expr
  final case class OpNil()                                    extends Expr
  final case class TabUnit()                                  extends Expr
}
