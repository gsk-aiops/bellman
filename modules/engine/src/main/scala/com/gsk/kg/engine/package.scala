package com.gsk.kg

import cats.data.Chain
import cats.data.Kleisli
import cats.data.ReaderWriterStateT

import org.apache.spark.sql.DataFrame

import com.gsk.kg.config.Config
import com.gsk.kg.sparqlparser.Result

package object engine {

  type Log = Chain[String]

  type M[A] = ReaderWriterStateT[Result, Config, Log, DataFrame, A]
  val M = ReaderWriterStateT

  /** [[Phase]] represents a phase in the compiler.  It's parametrized
    * on the input type [[A]] and the output type [[B]].
    */
  type Phase[A, B] = Kleisli[M, A, B]
  val Phase = Kleisli

}
