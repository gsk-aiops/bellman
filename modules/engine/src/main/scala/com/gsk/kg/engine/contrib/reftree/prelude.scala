package com.gsk.kg.engine.contrib.reftree

import higherkindness.droste._
import cats.Traverse
import cats.data.State
import reftree.core._
import higherkindness.droste.data.Fix
import com.gsk.kg.sparqlparser.Expr
import com.gsk.kg.engine.data.ChunkedList
import reftree.contrib.SimplifiedInstances._
import com.gsk.kg.engine.ExpressionF
import com.gsk.kg.sparqlparser.StringVal
import reftree.contrib.SimplifiedInstances

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

  implicit def chunkToRefTree[A: ToRefTree]: ToRefTree[ChunkedList.Chunk[A]] =
    ToRefTree(chunk =>
      RefTree.Ref(
        chunk.toChain,
        chunk.toChain.toList.map(_.refTree.toField)
      )
    )
}
