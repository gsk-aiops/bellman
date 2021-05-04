package com.gsk.kg.engine
package optimizer

import cats.implicits._

import higherkindness.droste.Basis

import com.gsk.kg.engine.DAG._
import com.gsk.kg.engine.data.ToTree._

object JoinBGPs {

  def apply[T](implicit T: Basis[DAG, T]): T => T = { t =>
    T.coalgebra(t).rewrite { case j @ Join(l, r) =>
      (T.coalgebra(l), T.coalgebra(r)) match {
        case (BGP(tl), BGP(tr)) => bgp(tl concat tr)
        case _                  => j
      }
    }
  }

  def phase[T](implicit T: Basis[DAG, T]): Phase[T, T] = Phase { t =>
    val result = apply(T)(t)
    (result != t)
      .pure[M]
      .ifM(
        Log.debug(
          "Optimizer(JoinBGPs)",
          s"resulting query: ${result.toTree.drawTree}"
        ),
        ().pure[M]
      ) *>
      result.pure[M]
  }

}
