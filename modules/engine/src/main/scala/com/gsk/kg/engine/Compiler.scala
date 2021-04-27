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
import com.gsk.kg.sparqlparser.Result

object Compiler {

  def compile(df: DataFrame, query: String, config: Config)(implicit
      sc: SQLContext
  ): Result[DataFrame] =
    compiler(df)
      .run(query)
      .runA(config, df)

  // scalastyle:off
  def explain(query: String)(implicit sc: SQLContext): Unit = {
    import sc.implicits._
    val df = List.empty[(String, String, String)].toDF("s", "p", "o")
    compiler(df)
      .run(query)
      .runA(Config.default, df) match {
      case Left(x)   => println(x)
      case Right(df) => df.explain(true)
    }
  }

  /** Put together all phases of the compiler
    *
    * @param df
    * @param sc
    * @return
    */
  private def compiler(df: DataFrame)(implicit
      sc: SQLContext
  ): Phase[String, DataFrame] =
    parser >>>
      transformToGraph.first >>>
      optimizer >>>
      staticAnalysis >>>
      engine(df) >>>
      rdfFormatter

  private def transformToGraph[T: Basis[DAG, *]]: Phase[Query, T] =
    Arrow[Phase].lift(DAG.fromQuery)

  /** The engine phase receives a query and applies it to the given
    * dataframe
    *
    * @param df
    * @param sc
    * @return
    */
  private def engine[T: Basis[DAG, *]](df: DataFrame)(implicit
      sc: SQLContext
  ): Phase[T, DataFrame] =
    Kleisli[M, T, DataFrame] { query =>
      M.ask[Result, Config, Log, DataFrame].flatMapF { config =>
        Engine.evaluate(df, query, config)
      }
    }

  /** parser converts strings to our [[Query]] ADT
    */
  private def parser: Phase[String, (Query, Graphs)] =
    Kleisli[M, String, (Query, Graphs)] { query =>
      M.ask[Result, Config, Log, DataFrame].flatMapF { config =>
        QueryConstruct.parse(query, config)
      }
    }

  private def optimizer[T: Basis[DAG, *]]: Phase[(T, Graphs), T] =
    Optimizer.optimize

  private def staticAnalysis[T: Basis[DAG, *]]: Phase[T, T] =
    Analyzer.analyze

  private def rdfFormatter: Phase[DataFrame, DataFrame] = {
    Kleisli[M, DataFrame, DataFrame] { inDf =>
      M.ask[Result, Config, Log, DataFrame].map { config =>
        RdfFormatter.formatDataFrame(inDf, config)
      }
    }
  }

}
