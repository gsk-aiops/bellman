package com.gsk.kg.engine.properties

import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import com.gsk.kg.engine.functions.FuncForms

object FuncProperty {

  def alternative(df: DataFrame, pel: Column, per: Column): Column = {
    val col = df("p")
    when(
      col.startsWith("\"") && col.endsWith("\""),
      FuncForms.equals(trim(col, "\""), pel) ||
        FuncForms.equals(trim(col, "\""), per)
    ).otherwise(
      FuncForms.equals(col, pel) ||
        FuncForms.equals(col, per)
    )
  }

  def uri(s: String): Column = lit(s)
}
