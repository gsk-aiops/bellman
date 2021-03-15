package com.gsk.kg.engine
package optimizer

import com.gsk.kg.engine.data.ToTree._
import com.gsk.kg.engine.data.ChunkedList._

import cats.implicits._
import cats.arrow.Arrow

import higherkindness.droste.Basis
import higherkindness.droste.syntax.all._
import com.gsk.kg.engine.DAG._

object CompactBGPs {

  def apply[T](implicit T: Basis[DAG, T]): T => T = { t =>
    T.coalgebra(t).rewrite {
      case x @ BGP(triples) =>
        val t = triples.map(T.coalgebra.apply)

        BGP(
          t.compact({
            case Triple(s, p, o) => s.s
          }).map(_.embed)
        )
    }
  }

}
