package com.gsk.kg.engine

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext

package object syntax {

  implicit class SparQLSyntaxOnDataFrame(private val df: DataFrame)(implicit
      sc: SQLContext
  ) {
    def sparql(query: String): DataFrame =
      Compiler.compile(df, query).right.get
  }

}
