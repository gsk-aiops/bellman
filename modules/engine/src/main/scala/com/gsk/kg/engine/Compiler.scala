package com.gsk.kg.engine

import cats.arrow.Arrow
import cats.data.Kleisli
import cats.implicits._

import higherkindness.droste.Basis

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext

import com.gsk.kg.Graphs
import com.gsk.kg.config.Config
import com.gsk.kg.engine.analyzer.Analyzer
import com.gsk.kg.engine.optimizer.Optimizer
import com.gsk.kg.sparqlparser.Query
import com.gsk.kg.sparqlparser.QueryConstruct

object Compiler {

  def compile(df: DataFrame, query: String, config: Config)(implicit
      sc: SQLContext
  ): Result[DataFrame] =
    compiler(df, config)
      .run(query)
      .runA(df)

  /** Put together all phases of the compiler
    *
    * @param df
    * @param sc
    * @return
    */
  def compiler(df: DataFrame, config: Config)(implicit
      sc: SQLContext
  ): Phase[String, DataFrame] =
    parser(config) >>>
      transformToGraph.first >>>
      optimizer >>>
      staticAnalysis >>>
      engine(df) >>>
      rdfFormatter

  def transformToGraph[T: Basis[DAG, *]]: Phase[Query, T] =
    Arrow[Phase].lift(DAG.fromQuery)

  /** The engine phase receives a query and applies it to the given
    * dataframe
    *
    * @param df
    * @param sc
    * @return
    */
  def engine[T: Basis[DAG, *]](df: DataFrame)(implicit
      sc: SQLContext
  ): Phase[T, DataFrame] =
    Kleisli { case query =>
      M.liftF(Engine.evaluate(df, query))
    }

  /** parser converts strings to our [[Query]] ADT
    */
  def parser(config: Config): Phase[String, (Query, Graphs)] =
    Arrow[Phase].lift(query => QueryConstruct.parse(query, config))

  def optimizer[T: Basis[DAG, *]]: Phase[(T, Graphs), T] =
    Optimizer.optimize

  def staticAnalysis[T: Basis[DAG, *]]: Phase[T, T] =
    Analyzer.analyze

  def rdfFormatter: Phase[DataFrame, DataFrame] =
    Arrow[Phase].lift(RdfFormatter.formatDataFrame)

}
