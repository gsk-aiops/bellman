package com.gsk.kg.engine
package optimizer

import cats.arrow.Arrow
import cats.implicits._

import higherkindness.droste.Basis

import com.gsk.kg.Graphs

object Optimizer {

  def graphsPushdownPhase[T: Basis[DAG, *]]: Phase[(T, Graphs), T] =
    Phase { case (t, graphs) =>
      GraphsPushdown[T].apply(t, graphs).pure[M]
    }

  def optimize[T: Basis[DAG, *]]: Phase[(T, Graphs), T] =
    graphsPushdownPhase >>>
      Arrow[Phase].lift(CompactBGPs[T]) >>>
      Arrow[Phase].lift(RemoveNestedProject[T])
}
