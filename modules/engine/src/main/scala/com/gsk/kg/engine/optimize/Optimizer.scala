package com.gsk.kg.engine
package optimizer

import cats.arrow.Arrow
import cats.implicits._

import higherkindness.droste.Basis

import com.gsk.kg.engine.optimize.DefaultGraphsPushdown
import com.gsk.kg.sparqlparser.StringVal

object Optimizer {

  def defaultGraphsPushdown[T: Basis[DAG, *]]: Phase[(T, List[StringVal]), T] =
    Phase { case (t, defaults) =>
      DefaultGraphsPushdown[T].apply(t, defaults).pure[M]
    }
//  Arrow[Phase].lift[T => List[StringVal], T](DefaultGraphsPushdown[T])

  def optimize[T: Basis[DAG, *]]: Phase[(T, List[StringVal]), T] =
    defaultGraphsPushdown >>>
      Arrow[Phase].lift(NamedGraphPushdown[T]) >>>
      Arrow[Phase].lift(CompactBGPs[T]) >>>
      Arrow[Phase].lift(RemoveNestedProject[T])
}
