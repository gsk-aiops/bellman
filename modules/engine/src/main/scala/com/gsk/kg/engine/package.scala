package com.gsk.kg

import cats.data.StateT
import cats.data.Kleisli
import org.apache.spark.sql.DataFrame

package object engine {

  /** the type for operations that may fail
    */
  type Result[A] = Either[EngineError, A]
  val Result = Either

  type M[A] = StateT[Result, DataFrame, A]
  val M = StateT

  /** [[Phase]] represents a phase in the compiler.  It's parametrized
    * on the input type [[A]] and the output type [[B]].
    */
  type Phase[A, B] = Kleisli[M, A, B]
  val Phase = Kleisli

}
