package com.gsk.kg.engine

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext

import com.gsk.kg.config.Config

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
      Compiler.compile(df, query, config).right.get

    /** Compile query with dataframe with default configuration
      * @param query
      * @return
      */
    def sparql(query: String): DataFrame =
      sparql(query, Config.default)
  }

}
