import StringVal.VARIABLE

sealed trait Expr

object Expr {
  final case class BGP(triples:Seq[Triple]) extends Expr
  final case class Triple(s:StringVal, p:StringVal, o:StringVal) extends Expr
  final case class LeftJoin(l:Expr, r:Expr) extends Expr
  final case class FilteredLeftJoin(l:Expr, r:Expr, f:FilterFunction) extends Expr
  final case class Union(l:Expr, r:Expr) extends Expr
  final case class Extend(bindTo:StringLike, bindFrom:StringLike, r:Expr) extends Expr
  final case class Filter(funcs:Seq[FilterFunction], expr:Expr) extends Expr
  final case class Join(l:Expr, r:Expr) extends Expr
  final case class Graph(g:StringVal, e:Expr) extends Expr
  final case class Construct(vars: Seq[VARIABLE], bgp: BGP, r:Expr) extends Expr
  final case class Select(vars: Seq[VARIABLE], r:Expr) extends Expr

  
}
