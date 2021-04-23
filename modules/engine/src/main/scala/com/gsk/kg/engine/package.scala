package com.gsk.kg

import cats.data.Chain
import cats.data.Kleisli
import cats.data.ReaderWriterStateT

import org.apache.spark.sql.DataFrame

import com.gsk.kg.config.Config

package object engine {

  type Log = Chain[String]

  /** the type for operations that may fail
    */
  type Result[A] = Either[EngineError, A]
  val Result = Either

  type M[A] = ReaderWriterStateT[Result, Config, Log, DataFrame, A]
  val M = ReaderWriterStateT

  /** [[Phase]] represents a phase in the compiler.  It's parametrized
    * on the input type [[A]] and the output type [[B]].
    */
  type Phase[A, B] = Kleisli[M, A, B]
  val Phase = Kleisli

}
