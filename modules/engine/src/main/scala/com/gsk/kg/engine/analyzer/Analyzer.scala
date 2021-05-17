package com.gsk.kg.engine
package analyzer

import cats.Foldable
import cats.data.Validated._
import cats.data.ValidatedNec
import cats.implicits._

import higherkindness.droste.Basis

import org.apache.spark.sql.DataFrame

import com.gsk.kg.config.Config
import com.gsk.kg.sparqlparser.EngineError
import com.gsk.kg.sparqlparser.Result

object Analyzer {

  def rules[T: Basis[DAG, *]]: List[Rule[T]] =
    List(FindUnboundVariables[T])

  /** Execute all rules in [[Analyzer.rules]] and accumulate errors
    * that they may throw.
    *
    * In case no errors are returned, the
    *
    * @return
    */
  def analyze[T: Basis[DAG, *]]: Phase[T, T] =
    Phase { t =>
      val x: ValidatedNec[String, String] = Foldable[List].fold(rules.map(_(t)))

      x match {
        case Invalid(e) =>
          e.traverse(err => Log.error("Analyzer", err)) *>
            M.liftF[Result, Config, Log, DataFrame, T](
              EngineError.AnalyzerError(e).asLeft
            )
        case Valid(e) => t.pure[M]
      }
    }

}
