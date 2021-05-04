package com.gsk.kg.engine

import cats.data.Kleisli
import cats.syntax.either._
import cats.implicits._

import higherkindness.droste.Basis

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext

import com.gsk.kg.Graphs
import com.gsk.kg.config.Config
import com.gsk.kg.engine.data.ToTree._
import com.gsk.kg.engine.analyzer.Analyzer
import com.gsk.kg.engine.optimizer.Optimizer
import com.gsk.kg.sparqlparser.Query
import com.gsk.kg.sparqlparser.QueryConstruct
import com.gsk.kg.sparqlparser.Result

object Compiler {

  // scalastyle:off
  def compile(df: DataFrame, query: String, config: Config)(implicit
      sc: SQLContext
  ): Result[DataFrame] =
    compiler(df)
      .run(query)
      .run(config, df)
      .map { case (log, _, df) =>
        Log.run(log)
        df
      }

  // scalastyle:off
  def explain(query: String)(implicit sc: SQLContext): Unit = {
    import sc.implicits._
    val df = List.empty[(String, String, String)].toDF("s", "p", "o")
    compiler(df)
      .run(query)
      .run(Config.default, df) match {
      case Left(x) => println(x)
      case Right((log, _, df)) =>
        Log.run(log)
        df.explain(true)
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
      staticAnalysis.first >>>
      optimizer >>>
      engine(df) >>>
      rdfFormatter

  private def transformToGraph[T: Basis[DAG, *]]: Phase[Query, T] =
    Kleisli[M, Query, T] { query =>
      for {
        _   <- Log.info("TransformToGraph", "transforming Query to DAG")
        dag <- DAG.fromQuery.apply(query).pure[M]
        _ <- Log.debug(
          "TransformToGraph",
          s"resulting dag: \n${dag.toTree.drawTree}"
        )
      } yield dag
    }

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
      Log.info("Engine", "Running the engine") *>
        M.ask[Result, Config, Log, DataFrame].flatMapF { config =>
          Engine.evaluate(df, query, config)
        }
    }

  /** parser converts strings to our [[Query]] ADT
    */
  private def parser: Phase[String, (Query, Graphs)] =
    Kleisli[M, String, (Query, Graphs)] { query =>
      Log.info("Parser", "Running the parser") *>
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
      Log.info("RdfFormatter", "Running the RDF formatter") *>
        M.ask[Result, Config, Log, DataFrame].map { config =>
          RdfFormatter.formatDataFrame(inDf, config)
        }
    }
  }

}
