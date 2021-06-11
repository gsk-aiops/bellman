package com.gsk.kg.site.contrib.reftree

import cats.Traverse
import cats.data.State
import cats.implicits._

import higherkindness.droste._
import higherkindness.droste.data.Fix

import com.gsk.kg.engine.DAG
import com.gsk.kg.engine.data.ChunkedList
import com.gsk.kg.sparqlparser.ConditionOrder
import com.gsk.kg.sparqlparser.ConditionOrder.ASC
import com.gsk.kg.sparqlparser.ConditionOrder.DESC
import com.gsk.kg.sparqlparser.Expr

import reftree.contrib.SimplifiedInstances._
import reftree.core._

object prelude {

  def fixedToRefTreeAlgebra[F[_]](implicit
      evF: ToRefTree[F[RefTree]]
  ): Algebra[F, RefTree] =
    Algebra((fa: F[RefTree]) => evF.refTree(fa))

  final case class Zedd(
      level: Int,
      counters: Map[Int, Int]
  ) {

    def down: Zedd = copy(level = level + 1)
    def up: Zedd   = copy(level = level - 1)

    def next: (Zedd, Int) = {
      val nn = counters.get(level).fold(0)(_ + 1)
      copy(counters = counters + (level -> nn)) -> nn
    }
  }

  object Zedd {
    type M[A] = State[Zedd, A]

    def empty: Zedd = Zedd(0, Map.empty)

    def down[F[_], R](implicit
        project: higherkindness.droste.Project[F, R]
    ): CoalgebraM[M, F, R] =
      CoalgebraM[M, F, R](a => State(s => (s.down, project.coalgebra(a))))

    def up[F[_]](algebra: Algebra[F, RefTree]): AlgebraM[M, F, RefTree] =
      AlgebraM[M, F, RefTree](fa =>
        State { s =>
          val (ss, i) = s.next
          ss.up -> (algebra(fa) match {
            case ref: RefTree.Ref =>
              ref.copy(id = s"${ref.name}-${s.level}-$i")
            case other => other
          })
        }
      )
  }

  implicit def fixedToRefTree[F[_] <: AnyRef: Traverse](implicit
      ev: ToRefTree[F[RefTree]]
  ): ToRefTree[Fix[F]] =
    ToRefTree(input =>
      scheme
        .hyloM[Zedd.M, F, Fix[F], RefTree](
          Zedd.up(fixedToRefTreeAlgebra),
          Zedd.down
        )
        .apply(input)
        .runA(Zedd.empty)
        .value
    )

  implicit val quadToRefTree: ToRefTree[Expr.Quad] = ToRefTree(tree =>
    RefTree.Ref(
      tree,
      Seq(
        tree.s.s.refTree.toField.withName("s"),
        tree.p.s.refTree.toField.withName("p"),
        tree.o.s.refTree.toField.withName("o"),
        tree.g.mkString.refTree.toField.withName("g")
      )
    )
  )

  implicit val conditionOrderRefTree: ToRefTree[ConditionOrder] = ToRefTree {
    case tree @ ASC(e) =>
      RefTree.Ref(tree, Seq(e.refTree.toField.withName("Asc")))
    case tree @ DESC(e) =>
      RefTree.Ref(tree, Seq(e.refTree.toField.withName("Desc")))
  }

  implicit def chunkToRefTree[A: ToRefTree]: ToRefTree[ChunkedList.Chunk[A]] =
    ToRefTree(chunk =>
      RefTree.Ref(
        chunk.toChain,
        chunk.toChain.toList.map(_.refTree.toField)
      )
    )

  implicit def chunkedListToRefTree[A: ToRefTree]: ToRefTree[ChunkedList[A]] =
    ToRefTree(list =>
      RefTree.Ref(
        list,
        list.mapChunks(_.refTree.toField).toList
      )
    )

  implicit val dagToRefTree: ToRefTree[DAG[RefTree]] = ToRefTree[DAG[RefTree]] {
    case dag @ DAG.Describe(vars, r) =>
      RefTree.Ref(dag, vars.map(_.refTree.toField) ++ Seq(r.toField))
    case dag @ DAG.Ask(r) => RefTree.Ref(dag, Seq(r.toField))
    case dag @ DAG.Construct(bgp, r) =>
      RefTree.Ref(
        dag,
        Seq(
          DAG
            .bgp[RefTree](ChunkedList.fromList(bgp.quads.toList))
            .refTree
            .toField,
          r.toField
        )
      )
    case dag @ DAG.Scan(graph, expr) =>
      RefTree.Ref(dag, Seq(graph.refTree.toField, expr.toField))
    case dag @ DAG.Project(variables, r) =>
      RefTree.Ref(dag, variables.map(_.refTree.toField) ++ Seq(r.toField))
    case dag @ DAG.Bind(variable, expression, r) =>
      RefTree.Ref(
        dag,
        Seq(
          variable.refTree.toField,
          expression.toString.refTree.toField,
          r.toField
        )
      )
    case dag @ DAG.BGP(quads) =>
      RefTree.Ref(dag, Seq(quads.refTree.toField))
    case dag @ DAG.LeftJoin(l, r, filters) =>
      RefTree.Ref(dag, Seq(l.toField, r.toField))
    case dag @ DAG.Union(l, r) => RefTree.Ref(dag, Seq(l.toField, r.toField))
    case dag @ DAG.Filter(funcs, expr) =>
      RefTree.Ref(dag, Seq(funcs.toString.refTree.toField, expr.toField))
    case dag @ DAG.Join(l, r) => RefTree.Ref(dag, Seq(l.toField, r.toField))
    case dag @ DAG.Offset(o, r) =>
      RefTree.Ref(dag, Seq(o.refTree.toField, r.toField))
    case dag @ DAG.Limit(l, r) =>
      RefTree.Ref(dag, Seq(l.refTree.toField, r.toField))
    case dag @ DAG.Distinct(r) => RefTree.Ref(dag, Seq(r.toField))
    case dag @ DAG.Reduced(r)  => RefTree.Ref(dag, Seq(r.toField))
    case dag @ DAG.Group(vars, func, r) =>
      RefTree.Ref(dag, Seq(vars.refTree.toField, r.toField))
    case dag @ DAG.Order(conds, r) =>
      RefTree.Ref(dag, Seq(conds.refTree.toField, r.toField))
    case dag @ DAG.Minus(l, r) =>
      RefTree.Ref(dag, Seq(l.toField, r.toField))
    case dag @ DAG.Table(vars, rows) =>
      RefTree.Ref(dag, vars.map(_.refTree.toField))
    case dag @ DAG.Exists(not, p, r) =>
      RefTree.Ref(dag, Seq(not.toString.refTree.toField, p.toField, r.toField))
    case dag @ DAG.Noop(str) => RefTree.Ref(dag, Seq())
  }
}
