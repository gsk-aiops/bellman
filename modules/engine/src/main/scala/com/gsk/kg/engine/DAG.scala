package com.gsk.kg.engine

import higherkindness.droste._
import higherkindness.droste.data.Fix
import higherkindness.droste.macros.deriveTraverse
import higherkindness.droste.syntax.all._
import higherkindness.droste.util.DefaultTraverse
import com.gsk.kg.sparqlparser.StringVal.VARIABLE
import com.gsk.kg.sparqlparser.StringVal
import cats._
import cats.data.NonEmptyList
import cats.implicits._
import com.gsk.kg.sparqlparser.Expr
import com.gsk.kg.sparqlparser.Expr.fixedpoint._
import com.gsk.kg.sparqlparser.Query
import com.gsk.kg.sparqlparser.Expression
import com.gsk.kg.engine.data.ChunkedList
import monocle._
import monocle.macros.Lenses


sealed trait DAG[A] {

  def rewrite(
      pf: PartialFunction[DAG[A], DAG[A]]
  )(implicit A: Basis[DAG, A]): A =
    scheme
      .cata(Trans(pf.orElse(PartialFunction[DAG[A], DAG[A]] { a =>
        a
      })).algebra)
      .apply(this.embed)

  def widen: DAG[A] = this
}

// scalastyle:off
object DAG {
  @Lenses final case class Describe[A](vars: List[VARIABLE], r: A) extends DAG[A]
  @Lenses final case class Ask[A](r: A) extends DAG[A]
  @Lenses final case class Construct[A](bgp: Expr.BGP, r: A) extends DAG[A]
  @Lenses final case class Scan[A](graph: String, expr: A) extends DAG[A]
  @Lenses final case class Project[A](variables: List[VARIABLE], r: A) extends DAG[A]
  @Lenses final case class Bind[A](variable: VARIABLE, expression: Expression, r: A)
      extends DAG[A]
  @Lenses final case class BGP[A](triples: ChunkedList[Expr.Triple]) extends DAG[A]
  @Lenses final case class LeftJoin[A](l: A, r: A, filters: List[Expression])
      extends DAG[A]
  @Lenses final case class Union[A](l: A, r: A) extends DAG[A]
  @Lenses final case class Filter[A](funcs: NonEmptyList[Expression], expr: A) extends DAG[A]
  @Lenses final case class Join[A](l: A, r: A) extends DAG[A]
  @Lenses final case class Offset[A](offset: Long, r: A) extends DAG[A]
  @Lenses final case class Limit[A](limit: Long, r: A) extends DAG[A]
  @Lenses final case class Distinct[A](r: A) extends DAG[A]
  @Lenses final case class Noop[A](trace: String) extends DAG[A]

  implicit val traverse: Traverse[DAG] = new DefaultTraverse[DAG] {
    def traverse[G[_]: Applicative, A, B](fa: DAG[A])(f: A => G[B]): G[DAG[B]] =
      fa match {
        case DAG.Describe(vars, r)     => f(r).map(describe(vars, _))
        case DAG.Ask(r)                => f(r).map(ask)
        case DAG.Construct(bgp, r)     => f(r).map(construct(bgp, _))
        case DAG.Scan(graph, expr)     => f(expr).map(scan(graph, _))
        case DAG.Project(variables, r) => f(r).map(project(variables, _))
        case DAG.Bind(variable, expression, r) =>
          f(r).map(bind(variable, expression, _))
        case DAG.BGP(triples)    => bgp(triples).pure[G]
        case DAG.LeftJoin(l, r, filters) =>
          (
            f(l),
            f(r)
          ).mapN(leftJoin(_, _, filters))
        case DAG.Union(l, r) => (f(l), f(r)).mapN(union)
        case DAG.Filter(funcs, expr) =>
          f(expr).map(filter(funcs, _))
        case DAG.Join(l, r) => (f(l), f(r)).mapN(join)
        case DAG.Offset(o, r) =>
          f(r).map(offset(o, _))
        case DAG.Limit(l, r) =>
          f(r).map(limit(l, _))
        case DAG.Distinct(r) => f(r).map(distinct)
        case DAG.Noop(str)   => noop(str).pure[G]
      }
  }

  // Smart constructors for better type inference (they return DAG[A] instead of the case class itself)
  def describe[A](vars: List[VARIABLE], r: A): DAG[A] = Describe[A](vars, r)
  def ask[A](r: A): DAG[A] = Ask[A](r)
  def construct[A](bgp: Expr.BGP, r: A): DAG[A] = Construct[A](bgp, r)
  def scan[A](graph: String, expr: A): DAG[A] = Scan[A](graph, expr)
  def project[A](variables: List[VARIABLE], r: A): DAG[A] =
    Project[A](variables, r)
  def bind[A](variable: VARIABLE, expression: Expression, r: A): DAG[A] =
    Bind[A](variable, expression, r)
  def bgp[A](triples: ChunkedList[Expr.Triple]): DAG[A] = BGP[A](triples)
  def leftJoin[A](l: A, r: A, filters: List[Expression]): DAG[A] =
    LeftJoin[A](l, r, filters)
  def union[A](l: A, r: A): DAG[A] = Union[A](l, r)
  def filter[A](funcs: NonEmptyList[Expression], expr: A): DAG[A] =
    Filter[A](funcs, expr)
  def join[A](l: A, r: A): DAG[A] = Join[A](l, r)
  def offset[A](offset: Long, r: A): DAG[A] =
    Offset[A](offset, r)
  def limit[A](limit: Long, r: A): DAG[A] =
    Limit[A](limit, r)
  def distinct[A](r: A): DAG[A] = Distinct[A](r)
  def noop[A](trace: String): DAG[A] = Noop[A](trace)

  // Smart constructors for building the recursive version directly
  def describeR[T: Embed[DAG, *]](vars: List[VARIABLE], r: T): T =
    describe[T](vars, r).embed
  def askR[T: Embed[DAG, *]](r: T): T = ask[T](r).embed
  def constructR[T: Embed[DAG, *]](bgp: Expr.BGP, r: T): T =
    construct[T](bgp, r).embed
  def scanR[T: Embed[DAG, *]](graph: String, expr: T): T =
    scan[T](graph, expr).embed
  def projectR[T: Embed[DAG, *]](variables: List[VARIABLE], r: T): T =
    project[T](variables, r).embed
  def bindR[T: Embed[DAG, *]](
      variable: VARIABLE,
      expression: Expression,
      r: T
  ): T = bind[T](variable, expression, r).embed
  def bgpR[T: Embed[DAG, *]](triples: ChunkedList[Expr.Triple]): T = bgp[T](triples).embed
  def leftJoinR[T: Embed[DAG, *]](
      l: T,
      r: T,
      filters: List[Expression]
  ): T = leftJoin[T](l, r, filters).embed
  def unionR[T: Embed[DAG, *]](l: T, r: T): T = union[T](l, r).embed
  def filterR[T: Embed[DAG, *]](funcs: NonEmptyList[Expression], expr: T): T =
    filter[T](funcs, expr).embed
  def joinR[T: Embed[DAG, *]](l: T, r: T): T = join[T](l, r).embed
  def offsetR[T: Embed[DAG, *]](
      o: Long,
      r: T
  ): T = offset[T](o, r).embed
  def limitR[T: Embed[DAG, *]](
      l: Long,
      r: T
  ): T = limit[T](l, r).embed
  def distinctR[T: Embed[DAG, *]](r: T): T = distinct[T](r).embed
  def noopR[T: Embed[DAG, *]](trace: String): T = noop[T](trace).embed

  /**
    * Transform a [[Query]] into its [[Fix[DAG]]] representation
    *
    * @param query
    * @return
    */
  def fromQuery[T: Basis[DAG, *]]: Query => T = {
    case Query.Describe(vars, r, _, _) =>
      describeR(vars.toList, fromExpr[T].apply(r))
    case Query.Ask(r, _, _) => askR(fromExpr[T].apply(r))
    case Query.Construct(vars, bgp, r, _, _) =>
      constructR(bgp, fromExpr[T].apply(r))
    case Query.Select(vars, r, _, _) => projectR(vars.toList, fromExpr[T].apply(r))
  }

  def fromExpr[T: Basis[DAG, *]]: Expr => T = scheme.cata(transExpr.algebra)

  def transExpr[T](implicit T: Basis[DAG, T]): Trans[ExprF, DAG, T] =
    Trans {
      case ExtendF(bindTo, bindFrom, r)      => bind(bindTo, bindFrom, r)
      case FilteredLeftJoinF(l, r, f)        => leftJoin(l, r, f.toList)
      case UnionF(l, r)                      => union(l, r)
      case BGPF(triples)                     => bgp(ChunkedList.fromList(triples.toList))
      case OpNilF()                          => noop("OpNilF not supported yet")
      case GraphF(g, e)                      => scan(g.s, e)
      case JoinF(l, r)                       => join(l, r)
      case LeftJoinF(l, r)                   => leftJoin(l, r, Nil)
      case ProjectF(vars, r)                 => project(vars.toList, r)
      case TripleF(s, p, o)                  => noop("TripleF not supported")
      case DistinctF(r)                      => distinct(r)
      case OffsetLimitF(None, None, r)       => T.coalgebra(r)
      case OffsetLimitF(None, Some(l), r)    => limit(l, r)
      case OffsetLimitF(Some(o), None, r)    => offset(o, r)
      case OffsetLimitF(Some(o), Some(l), r) => offset(o, limit(l, r).embed)
      case FilterF(funcs, expr)              => filter(NonEmptyList.fromListUnsafe(funcs.toList), expr)
      case TabUnitF()                        => noop("TabUnitF not supported yet")
    }

  implicit def dagEq[A: Eq]: Eq[DAG[A]] =
    Eq.fromUniversalEquals

  implicit val eqDelay: Delay[Eq, DAG] =
    λ[Eq ~> (Eq ∘ DAG)#λ] { eqA =>
      dagEq(eqA)
    }

}

object optics {
  import DAG._

  type T = Fix[DAG]

  val basisIso: Iso[T, DAG[T]] = Iso[T, DAG[T]] { t => Basis[DAG, T].coalgebra(t) } { dag => Basis[DAG, T].algebra(dag) }

  def _describe[F[_] <: DAG[_]]: Prism[DAG[T], DAG[T]] = Prism.partial[DAG[T], DAG[T]] { case dag @ Describe(vars, r) => dag } (identity)
  def _ask[F[_] <: DAG[_]]: Prism[DAG[T], DAG[T]] = Prism.partial[DAG[T], DAG[T]] { case dag @ Ask(r) => dag } (identity)
  def _construct[F[_] <: DAG[_]]: Prism[DAG[T], DAG[T]] = Prism.partial[DAG[T], DAG[T]] { case dag @ Construct(bgp: Expr.BGP, r) => dag } (identity)
  def _scan[F[_] <: DAG[_]]: Prism[DAG[T], DAG[T]] = Prism.partial[DAG[T], DAG[T]] { case dag @ Scan(graph: String, expr) => dag } (identity)
  def _project[F[_] <: DAG[_]]: Prism[DAG[T], DAG[T]] = Prism.partial[DAG[T], DAG[T]] { case dag @ Project(variables: List[VARIABLE], r) => dag } (identity)
  def _bind[F[_] <: DAG[_]]: Prism[DAG[T], DAG[T]] = Prism.partial[DAG[T], DAG[T]] { case dag @ Bind(variable: VARIABLE, expression: Expression, r) => dag } (identity)
  def _bgp[F[_] <: DAG[_]]: Prism[DAG[T], DAG[T]] = Prism.partial[DAG[T], DAG[T]] { case dag @ BGP(triples: ChunkedList[Expr.Triple]) => dag } (identity)
  def _leftjoin[F[_] <: DAG[_]]: Prism[DAG[T], DAG[T]] = Prism.partial[DAG[T], DAG[T]] { case dag @ LeftJoin(l, r, filters: List[Expression]) => dag } (identity)
  def _union[F[_] <: DAG[_]]: Prism[DAG[T], DAG[T]] = Prism.partial[DAG[T], DAG[T]] { case dag @ Union(l, r) => dag } (identity)
  def _filter[F[_] <: DAG[_]]: Prism[DAG[T], DAG[T]] = Prism.partial[DAG[T], DAG[T]] { case dag @ Filter(funcs: NonEmptyList[Expression], expr) => dag } (identity)
  def _join[F[_] <: DAG[_]]: Prism[DAG[T], DAG[T]] = Prism.partial[DAG[T], DAG[T]] { case dag @ Join(l, r) => dag } (identity)
  def _offset[F[_] <: DAG[_]]: Prism[DAG[T], DAG[T]] = Prism.partial[DAG[T], DAG[T]] { case dag @ Offset(offset: Long, r) => dag } (identity)
  def _limit[F[_] <: DAG[_]]: Prism[DAG[T], DAG[T]] = Prism.partial[DAG[T], DAG[T]] { case dag @ Limit(limit: Long, r) => dag } (identity)
  def _distinct[F[_] <: DAG[_]]: Prism[DAG[T], DAG[T]] = Prism.partial[DAG[T], DAG[T]] { case dag @ Distinct(r) => dag } (identity)
  def _noop[F[_] <: DAG[_]]: Prism[DAG[T], DAG[T]] = Prism.partial[DAG[T], DAG[T]] { case dag @ Noop(trace: String) => dag } (identity)

  def _describeFocus: Prism[DAG[T], Describe[T]] = Prism.partial[DAG[T], Describe[T]] { case dag @ Describe(vars, r) => dag } (identity)
  def _askFocus: Prism[DAG[T], Ask[T]] = Prism.partial[DAG[T], Ask[T]] { case dag @ Ask(r) => dag } (identity)
  def _constructFocus: Prism[DAG[T], Construct[T]] = Prism.partial[DAG[T], Construct[T]] { case dag @ Construct(bgp: Expr.BGP, r) => dag } (identity)
  def _scanFocus: Prism[DAG[T], Scan[T]] = Prism.partial[DAG[T], Scan[T]] { case dag @ Scan(graph: String, expr) => dag } (identity)
  def _projectFocus: Prism[DAG[T], Project[T]] = Prism.partial[DAG[T], Project[T]] { case dag @ Project(variables: List[VARIABLE], r) => dag } (identity)
  def _bindFocus: Prism[DAG[T], Bind[T]] = Prism.partial[DAG[T], Bind[T]] { case dag @ Bind(variable: VARIABLE, expression: Expression, r) => dag } (identity)
  def _bgpFocus: Prism[DAG[T], BGP[T]] = Prism.partial[DAG[T], BGP[T]] { case dag @ BGP(triples: ChunkedList[Expr.Triple]) => dag } (identity)
  def _leftjoinFocus: Prism[DAG[T], LeftJoin[T]] = Prism.partial[DAG[T], LeftJoin[T]] { case dag @ LeftJoin(l, r, filters: List[Expression]) => dag } (identity)
  def _unionFocus: Prism[DAG[T], Union[T]] = Prism.partial[DAG[T], Union[T]] { case dag @ Union(l, r) => dag } (identity)
  def _filterFocus: Prism[DAG[T], Filter[T]] = Prism.partial[DAG[T], Filter[T]] { case dag @ Filter(funcs: NonEmptyList[Expression], expr) => dag } (identity)
  def _joinFocus: Prism[DAG[T], Join[T]] = Prism.partial[DAG[T], Join[T]] { case dag @ Join(l, r) => dag } (identity)
  def _offsetFocus: Prism[DAG[T], Offset[T]] = Prism.partial[DAG[T], Offset[T]] { case dag @ Offset(offset: Long, r) => dag } (identity)
  def _limitFocus: Prism[DAG[T], Limit[T]] = Prism.partial[DAG[T], Limit[T]] { case dag @ Limit(limit: Long, r) => dag } (identity)
  def _distinctFocus: Prism[DAG[T], Distinct[T]] = Prism.partial[DAG[T], Distinct[T]] { case dag @ Distinct(r) => dag } (identity)
  def _noopFocus: Prism[DAG[T], Noop[T]] = Prism.partial[DAG[T], Noop[T]] { case dag @ Noop(trace: String) => dag } (identity)

  val _describeR: Prism[T, Describe[T]] = basisIso composePrism _describeFocus
  val _askR: Prism[T, Ask[T]] = basisIso composePrism _askFocus
  val _constructR: Prism[T, Construct[T]] = basisIso composePrism _constructFocus
  val _scanR: Prism[T, Scan[T]] = basisIso composePrism _scanFocus
  val _projectR: Prism[T, Project[T]] = basisIso composePrism _projectFocus
  val _bindR: Prism[T, Bind[T]] = basisIso composePrism _bindFocus
  val _bgpR: Prism[T, BGP[T]] = basisIso composePrism _bgpFocus
  val _leftjoinR: Prism[T, LeftJoin[T]] = basisIso composePrism _leftjoinFocus
  val _unionR: Prism[T, Union[T]] = basisIso composePrism _unionFocus
  val _filterR: Prism[T, Filter[T]] = basisIso composePrism _filterFocus
  val _joinR: Prism[T, Join[T]] = basisIso composePrism _joinFocus
  val _offsetR: Prism[T, Offset[T]] = basisIso composePrism _offsetFocus
  val _limitR: Prism[T, Limit[T]] = basisIso composePrism _limitFocus
  val _distinctR: Prism[T, Distinct[T]] = basisIso composePrism _distinctFocus
  val _noopR: Prism[T, Noop[T]] = basisIso composePrism _noopFocus
}
// scalastyle:on
