package com.gsk.kg.engine
package optimizer

import cats.implicits._

import higherkindness.droste.Basis

import com.gsk.kg.Graphs

object Optimizer {

  def optimize[T: Basis[DAG, *]]: Phase[(T, Graphs), T] =
    GraphsPushdown.phase[T] >>>
      JoinBGPs.phase[T] >>>
      // TODO: deactivate compaction due to AIPL-3255, causing some AnalysisErrors in Spark
      // CompactBGPs.phase[T] >>>
      RemoveNestedProject.phase[T] >>>
      SubqueryPushdown.phase[T]

}
