package com.gsk.kg.engine.properties

import cats.implicits.catsSyntaxEitherId
import cats.syntax.either._

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

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

    val resultR: Result[DataFrame] = (per match {
      case Left(col) =>
        df.filter(df("p") === col)
      case Right(df) =>
        df
    }).withColumnRenamed("s", "sr")
      .withColumnRenamed("p", "pr")
      .withColumnRenamed("o", "or")
      .asRight[EngineError]

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

  def oneOrMore(df: DataFrame, e: ColOrDf): Result[ColOrDf] = {

    val i    = 1
    val sIdx = 1
    val pIdx = 2
    val oIdx = 3

    val filteredDf = (e match {
      case Right(predDf) => predDf
      case Left(predCol) => df.filter(predCol <=> df("p"))
    }).withColumnRenamed("s", s"$sIdx")
      .withColumnRenamed("p", s"$pIdx")
      .withColumnRenamed("o", s"$oIdx")

    val traversedDf = traverse(filteredDf, filteredDf, i, sIdx, pIdx, oIdx)

    val traversedCols = traversedDf.columns.toSeq.sorted

    val expandedPathsDf = merge(traversedDf, filteredDf.drop("g"))
      .distinct()
      .select(
        traversedCols.head,
        traversedCols.tail: _*
      )

    val collapsedPathsDf = collapsePathsDf(expandedPathsDf)

    Right(collapsedPathsDf).asRight[EngineError]
  }

  def uri(s: String): ColOrDf =
    Left(lit(s))

  /** It receives a base dataframe with all triples of the graph and creates a new dataframe which will contain all
    * existing paths. This is done by iterating and accumulating results for every new depth level until there are no
    * more triples to connect.
    * @param baseDf Base dataframe with all the triples unconnected
    * @param accDf The dataframe which it will accumulate every depth level of paths
    * @param i Iteration index
    * @param sIdx Subject index (for column renaming)
    * @param pIdx Predicate index (for column renaming)
    * @param oIdx Object index (for column renaming
    * @return Dataframe with all the triples connected creating paths
    */
  private def traverse(
      baseDf: DataFrame,
      accDf: DataFrame,
      i: Int,
      sIdx: Int,
      pIdx: Int,
      oIdx: Int
  ): DataFrame = {

    def step(
        baseDf: DataFrame,
        accDf: DataFrame,
        i: Int
    ): DataFrame = {

      val colInc: Int => Int = x => x + (2 * i)

      val accColNames = (sIdx to colInc(oIdx)).map(_.toString)

      val acc = accDf

      val baseRenamed = baseDf
        .withColumnRenamed(s"$oIdx", s"${colInc(oIdx)}")
        .withColumnRenamed(s"$pIdx", s"${colInc(pIdx)}")
        .withColumnRenamed(s"$sIdx", s"${colInc(sIdx)}")

      val newAcc = acc
        .join(
          baseRenamed,
          Seq(s"${colInc(sIdx)}"),
          "left_outer"
        )
        .select(accColNames.head, accColNames.tail: _*)

      newAcc
    }

    val stepDf = step(baseDf, accDf, i)

    val continueTraversing = !stepDf
      .filter(stepDf(s"${oIdx + 2}").isNotNull)
      .isEmpty

    if (continueTraversing) {
      step(baseDf, stepDf, i + 1)
    } else {
      accDf
    }
  }

  /** It merges two dataframes by adding rows from the right dataframe to the second and setting to null those columns
    * that are not present in the right one.
    * @param df1
    * @param df2
    * @return
    */
  private def merge(df1: DataFrame, df2: DataFrame): DataFrame = {

    def getNewColumns(column: Set[String], merged_cols: Set[String]) = {
      merged_cols.toList.map {
        case x if column.contains(x) => col(x)
        case x @ _                   => lit(null).as(x) // scalastyle:ignore
      }
    }

    val merged_cols = df1.columns.toSet ++ df2.columns.toSet
    val newDf1 =
      df1.select(getNewColumns(df1.columns.toSet, merged_cols): _*)
    val newDf2 =
      df2.select(getNewColumns(df2.columns.toSet, merged_cols): _*)

    newDf1.unionByName(newDf2)
  }

  /** It collapse a path dataframe by collecting the vertex of the ends of a path.
    * @param df
    * @return
    */
  private def collapsePathsDf(df: DataFrame): DataFrame = {

    implicit val spoEncoder: Encoder[Row] = RowEncoder(
      StructType(
        List(
          StructField("s", StringType),
          StructField("p", StringType),
          StructField("o", StringType)
        )
      )
    )

    df.map { r =>
      val (vs, es) = (1 until r.length)
        .foldLeft((Seq.empty[String], Seq.empty[String])) {
          case ((vs, es), idx) =>
            val col = r.getString(idx - 1)

            if (col == null) {
              (vs, es)
            } else {
              if (idx % 2 == 0) {
                (vs, es :+ col)
              } else {
                (vs :+ col, es)
              }
            }
        }
      Row(vs.head, es.head, vs.last)
    }
  }
}
