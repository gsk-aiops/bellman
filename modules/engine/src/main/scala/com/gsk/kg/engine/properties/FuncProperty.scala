package com.gsk.kg.engine.properties

import cats.implicits.catsSyntaxEitherId
import cats.syntax.either._

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import com.gsk.kg.engine.PropertyExpressionF.ColOrDf
import com.gsk.kg.engine.functions.FuncForms
import com.gsk.kg.sparqlparser.EngineError
import com.gsk.kg.sparqlparser.Result

object FuncProperty {

  def alternative(
      df: DataFrame,
      pel: ColOrDf,
      per: ColOrDf
  ): Result[ColOrDf] = {
    val col = df("p")

    (pel, per) match {
      case (Left(l), Left(r)) =>
        Left(
          when(
            col.startsWith("\"") && col.endsWith("\""),
            FuncForms.equals(trim(col, "\""), l) ||
              FuncForms.equals(trim(col, "\""), r)
          ).otherwise(
            FuncForms.equals(col, l) ||
              FuncForms.equals(col, r)
          )
        ).asRight
      case _ =>
        EngineError
          .InvalidPropertyPathArguments(
            s"Invalid arguments on property path: seq, pel: ${pel.toString}, per: ${per.toString}," +
              s" both should be of type column"
          )
          .asLeft
    }
  }

  def seq(
      df: DataFrame,
      pel: ColOrDf,
      per: ColOrDf
  ): Result[ColOrDf] = {

    val resultL: Result[DataFrame] = (pel match {
      case Left(col)    => df.filter(df("p") === col)
      case Right(accDf) => accDf
    })
      .withColumnRenamed("s", "sl")
      .withColumnRenamed("p", "pl")
      .withColumnRenamed("o", "ol")
      .asRight[EngineError]

    val resultR: Result[DataFrame] = per match {
      case Left(col) =>
        (df
          .filter(df("p") === col)
          .withColumnRenamed("s", "sr")
          .withColumnRenamed("p", "pr")
          .withColumnRenamed("o", "or"))
          .asRight[EngineError]
      case Right(df) =>
        EngineError
          .InvalidPropertyPathArguments(
            s"Invalid arguments on property path: seq, per: ${per.toString} should be of type column"
          )
          .asLeft[DataFrame]
    }

    for {
      l <- resultL
      r <- resultR
    } yield {
      Right(
        l
          .join(
            r,
            l("ol") <=> r("sr"),
            "inner"
          )
          .select(l("sl"), r("or"))
          .withColumnRenamed("sl", "s")
          .withColumnRenamed("or", "o")
      )
    }
  }

  def uri(s: String): ColOrDf =
    Left(lit(s))
}
