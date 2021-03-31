package com.gsk.kg.engine
package optimizer

import cats.arrow.Arrow
import cats.implicits._

import higherkindness.droste.Basis

import com.gsk.kg.engine.DAG

object Optimizer {

  def optimize[T: Basis[DAG, *]]: Phase[T, T] =
    Arrow[Phase].lift(NamedGraphPushdown[T]) >>>
      Arrow[Phase].lift(CompactBGPs[T]) >>>
      Arrow[Phase].lift(RemoveNestedProject[T])

}
