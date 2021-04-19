package com.gsk.kg.engine
package optimizer

import higherkindness.droste.Basis

import com.gsk.kg.engine.DAG._

object JoinBGPs {

  def apply[T](implicit T: Basis[DAG, T]): T => T =
    T.coalgebra(_).rewrite { case j @ Join(l, r) =>
      (T.coalgebra(l), T.coalgebra(r)) match {
        case (BGP(tl), BGP(tr)) => bgp(tl concat tr)
        case _                  => j
      }
    }

}
