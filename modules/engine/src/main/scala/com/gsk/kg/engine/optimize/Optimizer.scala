package com.gsk.kg.engine
package optimizer

import cats.arrow.Arrow
import cats.implicits._

import higherkindness.droste.Basis

import com.gsk.kg.engine.optimize.GraphsPushdown
import com.gsk.kg.sparqlparser.StringVal

object Optimizer {

  def graphsPushdownPhase[T: Basis[DAG, *]]: Phase[(T, List[StringVal]), T] =
    Phase { case (t, defaults) =>
      GraphsPushdown[T].apply(t, defaults).pure[M]
    }

  def optimize[T: Basis[DAG, *]]: Phase[(T, List[StringVal]), T] =
    graphsPushdownPhase >>>
      Arrow[Phase].lift(CompactBGPs[T]) >>>
      Arrow[Phase].lift(RemoveNestedProject[T])
}
