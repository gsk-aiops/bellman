package com.gsk.kg.engine
package analyzer

import cats.implicits._
import higherkindness.droste.Basis
import higherkindness.droste.syntax.all._
import cats.Foldable
import cats.data.ValidatedNec
import cats.data.Validated._
import org.apache.spark.sql.DataFrame
import optics._
import cats.Traverse
import com.gsk.kg.engine.data.ChunkedList
import com.gsk.kg.sparqlparser.StringVal.VARIABLE

import DAG._

object Analyzer {

  def rules[T: Basis[DAG, *]]: List[Rule[T]] =
    List(findUnboundVariables)


  /**
    * Execute all rules in [[Analyzer.rules]] and accumulate errors
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
        case Invalid(e) => M.lift[Result, DataFrame, T](EngineError.AnalyzerError(e).asLeft)
        case Valid(e) => t.pure[M]
      }
    }

  def findUnboundVariables[T](implicit T: Basis[DAG, T]): Rule[T] = { t =>
    val declaredVariables: Set[VARIABLE] = t.collect[List[List[VARIABLE]], List[VARIABLE]] {
        case BGP(triples) =>
          Traverse[ChunkedList].toList(triples).flatMap(_.getVariables).map(_._1.asInstanceOf[VARIABLE])
    }.flatten.toSet

    val usedVariablesInConstruct: Set[VARIABLE] =
      _constructR
        .composeLens(Construct.bgp)
        .getOption(t)
        .map(_.triples.flatMap(_.getVariables).map(_._1.asInstanceOf[VARIABLE]).toSet)
        .getOrElse(Set.empty)

    val usedVariablesInSelect: Set[VARIABLE] = _projectR.getOption(t).map(_.variables.toSet).getOrElse(Set.empty)

    val usedVariables = usedVariablesInSelect union usedVariablesInConstruct

    if (declaredVariables.union(usedVariables) != declaredVariables) {
      val msg = "found free variables " + usedVariables.diff(declaredVariables).mkString(", ")

      msg.invalidNec
    } else {
      "ok".validNec
    }
  }

}
