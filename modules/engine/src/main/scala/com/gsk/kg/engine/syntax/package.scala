package com.gsk.kg.engine

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext

import com.gsk.kg.config.Config
import com.gsk.kg.sparqlparser.EngineError

package object syntax {

  implicit class SparQLSyntaxOnDataFrame(private val df: DataFrame)(implicit
      sc: SQLContext
  ) {

    /** Compile query with dataframe with provided configuration
      * @param query
      * @param config
      * @return
      */
    def sparql(query: String, config: Config): DataFrame =
      Compiler.compile(df, query, config) match {
	      case Left(a) => throw EngineException(a)
	      case Right(b) => b
      }

    /** Compile query with dataframe with default configuration
      * @param query
      * @return
      */
    def sparql(query: String): DataFrame =
      sparql(query, Config.default)
  }

  case class EngineException(error: EngineError) extends RuntimeException(error.toString())

}
